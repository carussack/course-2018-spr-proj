from urllib.request import urlopen
import json
import dml
import prov.model
import datetime
import uuid

class getSidewalkInventory(dml.Algorithm):
    contributor = 'crussack'
    reads = []
    writes = ['crussack.getSidewalkInventory', 'crussack.getUniversities', 'crussack.getTrafficData', 'crussack.getBusData',]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('crussack', 'crussack')

        #sidewalk data
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/6aa3bdc3ff5443a98d506812825c250a_0.geojson'
        response = urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getSidewalkInventory")
        repo.createCollection("getSidewalkInventory")
        repo['crussack.getSidewalkInventory'].insert_many(r)
        #repo['crussack.getSidewalkInventory'].metadata({'complete':True})
        #print(repo['crussack.getSidewalkInventory'].metadata())
        
        #university data
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getUniversities")
        repo.createCollection("getUniversities")
        repo['crussack.getUniversities'].insert_many(r['result']['records'])
        #repo['crussack.getUniversities'].metadata({'complete':True})
        #print(repo['crussack.getUniversities'].metadata())

        #traffic data
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getTrafficData")
        repo.createCollection("getTrafficData")
        repo['crussack.getTrafficData'].insert_many(r['features'])
        #repo['crussack.getTrafficData'].metadata({'complete':True})
        #print(repo['crussack.getTrafficData'].metadata())
        
        #mbta bus data
        url = 'http://realtime.mbta.com/developer/api/v2/routes?api_key='+api_key+'&format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumbs(r, sort_keys=True, indent=2)
        repo.dropCollection("getBusData")
        repo.createCollection("getBusData")
        repo['crussack.getBusData'].insert_many(r)

        #street light data
        url = 'https://data.boston.gov/datastore/odata3.0/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5?$top=100&$format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getLightData")
        repo.createCollection("getLightData")
        repo['crussack.getLightData'].insert_many(r['value'])

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('crussack', 'crussack')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:crussack#getSidewalkInventory', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        sidewalk_resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Sidewalk Inventory', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        university_resource = doc.entity('bdp:208dc980-a278-49e3-b95b-e193bb7bb6e4', {'prov:label':'Boston Universities and Colleges', prov.model.PROV_TYPE:'ont:DataResource'})
        traffic_resource = doc.entity('bdp:de08c6fe69c942509089e6db98c716a3_0', {'prov:label':'Traffic Signal Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        bus_resource = doc.entity('mbta:route', {'prov:label':'mbta route list', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        streetlight_resource = doc.entity('bdp:c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5', {'prov:label':'Boston Streetlights', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_sidewalk = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_university = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_traffic = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bus = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_streetlight = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)


        doc.wasAssociatedWith(get_sidewalk, this_script)
        doc.wasAssociatedWith(get_university, this_script)
        doc.wasAssociatedWith(get_traffic, this_script)
        doc.wasAssociatedWith(get_bus, this_script)
        doc.wasAssociatedWith(get_streetlight, this_script)

        doc.usage(get_sidewalk, sidewalk_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  #'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                )
        doc.usage(get_university, university_resource, startTime, None,
                 {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                 }
                )
        doc.usage(get_traffic, traffic_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                 }
                )
        doc.usage(get_bus, bus_resource, startTime, None,
                 {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                 }
                )
        doc.usage(get_streetlight, streetlinght_resource, startTime, None,
                 {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                 }
                )

        getSidewalkInventory = doc.entity('dat:crussack#getSidewalkInventory', {prov.model.PROV_LABEL:'Sidewalk Inventory', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(getSidewalkInventory, this_script)
        doc.wasGeneratedBy(getSidewalkInventory, get_sidewalk, endTime)
        doc.wasDerivedFrom(getSidewalkInventory, sidewalk_resource, get_sidewalk, get_sidewalk, get_sidewalk)

        getUniversities = doc.entity('dat:crussack#getUniversities', {prov.model.PROV_LABEL:'Universities in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(getUniversities, this_script)
        doc.wasGeneratedBy(getUniversities, get_university, endTime)
        doc.wasDerivedFrom(getUniversities, university_resource, get_university, get_university, get_university)

        getTrafficData = doc.entity('dat:crussack#getTrafficData', {prov.model.PROV_LABEL:'Traffic Light Data in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(getTrafficData, this_script)
        doc.wasGeneratedBy(getTrafficData, get_traffic, endTime)
        doc.wasDerivedFrom(getTrafficData, traffic_resource, get_traffic, get_traffic, get_traffic)

        getBusData = doc.entity('dat:crussack#getBusData', {prov.model.PROV_LABEL:'MBTA Bus Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(getBusData, this_script)
        doc.wasGeneratedBy(getBusData, get_bus, endTime)
        doc.wasDerivedFrom(getBusData, bus_resource, get_bus, get_bus, get_bus)

        getLightData = doc.entity('dat:crussack#getLightData', {prov.model.PROV_LABEL:'StreetLights in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(getLightData, this_script)
        doc.wasGeneratedBy(getLightData, get_streetlight, endTime)
        doc.wasDerivedFrom(getLightData, streetlight_resource, get_streetlight, get_streetlight, get_streetlight)

        repo.logout()
                  
        return doc

#getSidewalkInventory.execute()
#doc = getSidewalkInventory.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
