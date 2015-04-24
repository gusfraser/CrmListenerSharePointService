using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.SharePoint.Client;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.Xrm.Client;
using Microsoft.Xrm.Client.Services;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Security;
using System.Text.RegularExpressions;
using System.Threading;
using Xrm;

namespace CrmListenerSharePointWorkerRole
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public static Uri serviceBusUri;

        public override void Run()
        {
            TopicDescription td = new TopicDescription(CloudConfigurationManager.GetSetting("ServiceBus.CRM.Topic"));
            td.MaxSizeInMegabytes = 5120;
            td.DefaultMessageTimeToLive = new TimeSpan(0, 1, 0);

            string connectionString = CloudConfigurationManager.GetSetting("ServiceBus.ConnectionString");

            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            if (!namespaceManager.TopicExists(CloudConfigurationManager.GetSetting("ServiceBus.CRM.Topic")))
            {
                namespaceManager.CreateTopic(td);
            }

            if (!namespaceManager.SubscriptionExists(CloudConfigurationManager.GetSetting("ServiceBus.CRM.Topic"), CloudConfigurationManager.GetSetting("ServiceBus.CRM.Subscription")))
            {
                namespaceManager.CreateSubscription(
                    CloudConfigurationManager.GetSetting("ServiceBus.CRM.Topic"),
                    CloudConfigurationManager.GetSetting("ServiceBus.CRM.Subscription"));
            }

            ReceiveMessages();
        }

        public void ReceiveMessages()
        {
            string connectionString = CloudConfigurationManager.GetSetting("ServiceBus.ConnectionString");

           SubscriptionClient Client = SubscriptionClient.CreateFromConnectionString
                        (connectionString, 
                        CloudConfigurationManager.GetSetting("ServiceBus.CRM.Topic"), CloudConfigurationManager.GetSetting("ServiceBus.CRM.Subscription"), 
                        ReceiveMode.PeekLock);


            while (true)
            {
                var message = Client.Receive(TimeSpan.FromSeconds(15));

                if (message != null)
                {
                    var CrmEntityData = message.GetBody<RemoteExecutionContext>();
                    try
                    {

                        switch (CrmEntityData.PrimaryEntityName)
                        {
                          
                            case "quote":
                                handleQuoteUpdate(CrmEntityData);
                                break;
                            case "account":
                                handleAccountUpdate(CrmEntityData);
                                break;
                            default:
                                break;
                        }

                        message.Complete(); //Complete means we have done our bit and handled the message
                        EventLog.WriteEntry("Application", string.Format(@"message completed for {0}, primary entity id = {1}", CrmEntityData.PrimaryEntityName, CrmEntityData.PrimaryEntityId), EventLogEntryType.Information);
                    }
                    catch (Exception ex)
                    {
                        message.Abandon(); //Abandon leaves the message on the queue
                        EventLog.WriteEntry("Application", string.Format(
@"message = {0}
source = {1}
stack trace = {2}
primary entity type = {3}
primary entity id = {4}", ex.Message, ex.Source, ex.StackTrace, CrmEntityData.PrimaryEntityName, CrmEntityData.PrimaryEntityId), EventLogEntryType.Error);
                    }
                }
            }
        }

        private void handleAccountUpdate(RemoteExecutionContext CrmEntityData)
        {
            // TODO: Update client site in SharePoint
            CrmConnection crmConnection = CrmConnection.Parse(CloudConfigurationManager.GetSetting("CRM.ConnectionString"));
            OrganizationService service = new OrganizationService(crmConnection);
            XrmServiceContext context = new XrmServiceContext(service);

            var accounts = context.AccountSet.Where(c => c.Id == CrmEntityData.PrimaryEntityId);

            foreach (var account in accounts)
            {
                AddUpdateAccountSite(account, context, service);
            }

        }

        private void handleQuoteUpdate(RemoteExecutionContext CrmEntityData)
        {
            // TODO: Generate documents etc. 
            throw new NotImplementedException();
        }

        public void AddUpdateAccountSite(Account account, XrmServiceContext context, IOrganizationService service)
        {
            try
            {
 
                string accountName = account.Name;                

                string siteUri = string.Format("/{0}", EscapeURLPart(accountName));

                // TODO: Check if site has changed, for example.. lookup CRM audit if necesary (XRM stuff)
                CreateSite(accountName, EscapeURLPart(accountName));

                var newSite = CloudConfigurationManager.GetSetting("SharePoint.Client.SiteRoot") + siteUri + "/";

                var documentLibrariesForAccount = context.SharePointDocumentLocationSet.Where(t => t.RegardingObjectId == account.ToEntityReference());
                bool docLibraryExists = false;
                foreach (var library in documentLibrariesForAccount)
                {
                    docLibraryExists = true;
                }

                if (!docLibraryExists)
                {
                    var siteURL = newSite;
                    var site = context.SharePointSiteSet.Where(a => a.AbsoluteURL == siteURL).FirstOrDefault();
                    if (site == null)
                    {
                        site = new SharePointSite();
                        site.AbsoluteURL = siteURL;
                        site.Name = accountName;
                        context.AddObject(site);
                        context.SaveChanges();
                        site = context.SharePointSiteSet.Where(a => a.AbsoluteURL == siteURL).FirstOrDefault();
                    }
                    var docLibs = context.SharePointDocumentLocationSet.Where(t => t.RegardingObjectId == account.ToEntityReference());
                    var documentLibrary = docLibs.Where(a => a.RelativeUrl == "CRM").FirstOrDefault();
                    if (documentLibrary == null)
                    {                        
                        documentLibrary = new SharePointDocumentLocation();
                        documentLibrary.RegardingObjectId = account.ToEntityReference();
                        documentLibrary.RelativeUrl = "CRM";
                        documentLibrary.Name = "CRM";
                        documentLibrary.ParentSiteOrLocation = site.ToEntityReference();
                        context.AddObject(documentLibrary);
                        context.SaveChanges();
                    }
                    
                }
               
                return; 
      
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Create client site using SharePoint CSOM
        /// </summary>
        /// <param name="clientName"></param>
        /// <param name="escapedClientName"></param>
        private static void CreateSite(string clientName, string escapedClientName)
        {

            using (ClientContext clientContext = new ClientContext(CloudConfigurationManager.GetSetting("SharePoint.Client.SiteRoot")))
            {
                SecureString passWord = new SecureString();

                foreach (char c in CloudConfigurationManager.GetSetting("SharePoint.Password").ToCharArray()) passWord.AppendChar(c);

                clientContext.Credentials = new SharePointOnlineCredentials(CloudConfigurationManager.GetSetting("SharePoint.Username"), passWord);

                Web web = clientContext.Web;

                clientContext.Load(web);

                clientContext.ExecuteQuery();

                if (!WebExists(clientContext, web.Url, web.Url.TrimEnd('/') + "/" + escapedClientName))
                {
                    var create = new WebCreationInformation();
                    create.Title = clientName;
                    create.Url = escapedClientName;
                    create.UseSamePermissionsAsParentSite = true;
                    // This could be an OOTB template; use format {GUID}#Name e.g. {0c439c2b-81f7-4d81-a5d8-8eef7b660eaa}#SPEvo15 :
                    create.WebTemplate = CloudConfigurationManager.GetSetting("SharePoint.Client.Site.Template");
                    create.Description = "SharePoint Evolution Site Created by the Azure Service Bus";
                    create.Language = 1033;

                    var newWeb = web.Webs.Add(create);
                    clientContext.ExecuteQuery();

                    newWeb.Title = clientName;
                    newWeb.Update();

                    // Get the top navigatin node collection

                    NavigationNodeCollection topNodes = newWeb.Navigation.TopNavigationBar;
                    clientContext.Load(topNodes);
                    clientContext.ExecuteQuery();

                    topNodes[0].Title = clientName;
                    topNodes[0].Update();

                    clientContext.ExecuteQuery();
                }



            }

        }

        /// <summary>
        /// var siteExists = WebExists("http://host/sites/site/", "http://host/sites/site/subsite");
        /// </summary>
        /// <param name="siteUrl"></param>
        /// <param name="webUrl"></param>
        /// <returns></returns>
        public static bool WebExists(ClientContext context, string siteUrl, string webUrl)
        {
            // load up the root web object but only 
            // specifying the sub webs property to avoid 
            // unneeded network traffic
            var web = context.Web;
            context.Load(web, w => w.Webs);
            context.ExecuteQuery();
            // use a simple linq query to get any sub webs with the URL we want to check
            var subWeb = (from w in web.Webs where w.Url == webUrl select w).SingleOrDefault();
            if (subWeb != null)
            {
                // if found true
                return true;
            }

            // default to false...
            return false;
        }

        /// <summary>
        /// Clean URL for SharePoint..
        /// </summary>
        /// <param name="unescapedString"></param>
        /// <returns></returns>
        public static string EscapeURLPart(string unescapedString)
        {
            return Regex.Replace(unescapedString, @"([$#%\*:<>\?\/{}[]|~+-,()\.\-\\])", "").Replace("&", "and");
        }




    }
}
