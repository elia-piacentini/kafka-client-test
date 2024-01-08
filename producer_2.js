var Kafka = require('node-rdkafka');

var producer = new Kafka.Producer({
  'debug' : 'all',
  'security.protocol': 'sasl_ssl',
  'sasl.mechanism': 'SCRAM-SHA-256',
  'sasl.username': 'mlqfvhjj',
  'sasl.password': 'TGhbyX0-kbMN4oMWUWyJT44qTbAIRvBQ',
  'metadata.broker.list': 'glider.srvs.cloudkafka.com:9094',
  'dr_cb': true  //delivery report callback
});

var topicName = 'mlqfvhjj-cases';

//logging debug messages, if debug is enabled
producer.on('event.log', function(log) {
  console.log(log);
});

//logging all errors
producer.on('event.error', function(err) {
  console.error('Error from producer');
  console.error(err);
});

//counter to stop this sample after maxMessages are sent
var counter = 0;
var maxMessages = 10;

producer.on('delivery-report', function(err, report) {
  console.log('delivery-report: ' + JSON.stringify(report));
  counter++;
});

//Wait for the ready event before producing
producer.on('ready', function(arg) {
  console.log('producer ready.' + JSON.stringify(arg));

  const value = Buffer.from(JSON.stringify({
    "Country": "Italy",
    "XC_BusinessLine__c": "eCity",
    "XC_LegalEntity__c": "EnelSole",
    "XC_Segment__c": "B2G",
    "commitTimestamp": 1701077957000,
    "commitNumber": 11980379594762,
    "transactionKey": "0007ebe4-40dc-fd47-1a21-dd06ee5344ef",
    "recordId": "5007a00000RnyLqAAJ",
    "assetType": "Support",
    "caseType": "XC_B2G_EnelXItalia_Management_Case",
    "changeType": "CREATE",
    "data": {
        "SynchedYoUrban": "true",
        "Id": "5007a00000RnyLqAAJ",
        "faultTypeCode": "IG",
        "FaultType": "IG",
        "defectDescriptionCode": "IP_LampadaSpenta",
        "DefectDescription": "IP_LampadaSpenta",
        "defectTypeCode": "Guasto elettrico",
        "DefectType": "Guasto elettrico",
        "priorityCode": "0",
        "Priority": "Medium",
        "LastModifiedDate": "2023-11-27 09:39:17",
        "ClosedDate": "",
        "OpenedDate": "2023-11-27 09:38:39",
        "subTypeCode": "XC199",
        "SubType": "Material Repair",
        "subCauseCode": "ATCL003",
        "SubCause": "Fault",
        "reasonCode": "XCMOT002",
        "Reason": "Provision Service",
        "originCode": "CAN013",
        "Origin": "Yourban Portal",
        "statusCode": "ESTA001",
        "Status": "Open",
        "CaseNumber": "08310365",
        "ParentCase": "",
        "Asset": {
            "Id": "02i7a00000U7U6IAAV",
            "Name": "037006-C04052276",
            "TAM_AssetType__c": "Support",
            "SerialNumber": "037006-C04052276"
        },
        "Account": {
            "Id": "0017a00001DmxEUAAZ",
            "Name": "Comune di Rimini"
        }
    },
    "changedFields": []
}));
    producer.produce(topicName, -1, value, 'msg', Date.now(), "", [{}]);


  //need to keep polling for a while to ensure the delivery reports are received
  var pollLoop = setInterval(function() {
      producer.poll();
      if (counter === maxMessages) {
        clearInterval(pollLoop);
        producer.disconnect();
      }
    }, 1000);

});

producer.on('disconnected', function(arg) {
  console.log('producer disconnected. ' + JSON.stringify(arg));
});

//starting the producer
producer.connect();