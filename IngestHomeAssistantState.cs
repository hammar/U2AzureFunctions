using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Azure;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace U2
{
    public static class IngestHomeAssistantState
    {
        private static List<string> _numericDeviceClasses = new(){"illuminance","temperature"};
        private static List<string> _onOffReportingDeviceClasses = new(){"motion"};

        [FunctionName("IngestHomeAssistantState")]
        public static async Task Run([EventHubTrigger("u2-eventhub-homeassistant", Connection = "u2eventhub_U2AzureFunctionsPolicy_EVENTHUB")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    log.LogInformation($"Processing incoming message: {eventData.EventBody}");

                    JsonElement json = JsonDocument.Parse(eventData.EventBody).RootElement;

                    if (json.TryGetProperty("entity_id", out JsonElement entityIdElement) &&
                        json.TryGetProperty("state", out JsonElement stateElement) && 
                        json.TryGetProperty("attributes", out JsonElement attributesElement) &&
                        json.TryGetProperty("last_changed", out JsonElement lastChangedElement)) {

                        // Entity ID is, e.g., "sensor.hue_motion_sensor_4_illuminance" where the section preceeding 
                        // the period ('sensor') is called the domain and the section after it denotes the entity name.
                        // The former must not be changed but the latter is user-settable.
                        // Complete Entity IDs (e.g., "domain.entity_name") must be unique.
                        // Multiple sensors on the same physical device typically share the majority of the entity name
                        // but have different sensor-specific suffixes (e.g., 'illuminance' above), but this is not
                        // guaranteed.
                        string entityId = entityIdElement.GetString();
                        string[] entityIdComponents = entityId.Split('.', 2);
                        string domain = entityIdComponents[0];
                        string entityName = entityIdComponents[1];
                        // TODO: Consider unique vs amalgamated adressing of points, i.e., should DT model the Hue Sensor as a single twin or as multiple points?
                        string twinId = entityName;

                        // TODO: Understand difference between last_changed and last_updated
                        DateTime lastChanged = DateTime.Parse(lastChangedElement.GetString());

                        // Inbound state is always string, parsed later before JSON Patch is built
                        string state = stateElement.GetString();

                        // The Device Class governs how data is displayed in the Home Assistant GUI, i.e., the format of
                        // the data. Most sensors have this set but it is not mandatory. We only ingest data with Device Class
                        // defined, and parse the state string based on Device Class.
                        // See https://www.home-assistant.io/integrations/sensor and 
                        // https://www.home-assistant.io/integrations/binary_sensor/ for allowed values
                        if (attributesElement.TryGetProperty("device_class", out JsonElement deviceClassElement)) {

                            string deviceClass = deviceClassElement.GetString();

                            JsonPatchDocument twinUpdate = new JsonPatchDocument();
                            bool valueMapped = false;
                            if (_numericDeviceClasses.Contains(deviceClass)) {
                                if (double.TryParse(state, out double value)) {
                                   twinUpdate.AppendAdd("/lastKnownValue/value", value);
                                   twinUpdate.AppendAdd("/lastKnownValue/timestamp", lastChanged);
                                   valueMapped = true;
                                }
                            }
                            else if (_onOffReportingDeviceClasses.Contains(deviceClass)) {
                                bool value = state.ToLower() == "on";
                                twinUpdate.AppendAdd("/lastKnownValue/value", value);
                                twinUpdate.AppendAdd("/lastKnownValue/timestamp", lastChanged);
                                valueMapped = true;
                            }
                            
                            if (valueMapped) {
                                // Connect to ADT
                                string adtInstanceUrl = "https://u2-adt.api.neu.digitaltwins.azure.net"; 
                                var credential = new DefaultAzureCredential();
                                var client = new DigitalTwinsClient(new Uri(adtInstanceUrl), credential);
                                log.LogInformation($"Service client created - ready to go");

                                log.LogInformation($"Updating twin '{twinId}' with operation '{twinUpdate.ToString()}'");
                                await Task.Yield();
                                //await client.UpdateDigitalTwinAsync(twinId, twinUpdate);
                            }  
                        }
                    }
                }

                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
