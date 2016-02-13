/**
 * some js tools and utilities 
 * you can save those in a file in  mongo_datafiles_path/scripts directory then load as follows
 * load("scripts/xxxx.js")  
`* load("/data/db/scripts/xxxx.js")
 *
 */
function muTests() {
	// to call individual function from mongo shell:
	// db.loadServerScripts();
	// mt = new muTests;
	// mt.functionName(arg ....)
	this.timeIt = function (startTime) {
		if (startTime === undefined) {return new Date().getTime();}
		return (new Date().getTime() - startTime) / 1000;
	};
	this.strHash = function (str) {
		// return hash of a string
		var hash = 0, i, chr, len;
		if (str.length === 0) {return hash;}
		for (i = 0, len = str.length; i < len; i++) {
			chr   = str.charCodeAt(i);
			hash  = ((hash << 5) - hash) + chr;
			hash |= 0; // Convert to 32bit integer
		}
		return hash;
	}; 
	this.testDoc = function (n) {
		// produces a test doc with n+1 fields containing random strings
		var doc={}; 
		doc.lang=['da', 'de', 'en', 'es', 'fi', 'fr', 'hu', 'it', 'nl', 'pt', 'ro', 'ru', 'sv', 'tr'][Math.floor(Math.random() * 13)];
		for (var i = 0;
		i < n; i++) { doc['fld_'+i]=Math.random().toString(34).slice(2);} 
		return doc;
	};

	this.insertTestDocs = function (colName, nDocs, nFields) {
		// inserts n=nDocs testDoc containing n=nFields in collection=colName
		var tms = this.timeIt();
		for (var i = 0;
		i < nDocs; i++) {doc=this.testDoc(nFields); doc._id=i;
		db[colName].insert(doc);
		}
		seconds = this.timeIt(tms);
		return {seconds:seconds, docs_per_second: nDocs/seconds};
	};
	this.insertTestDocsArr = function (colName, nDocs, nFields) { 
		// same as insertTestDocs but inserts as an array
		var tms = this.timeIt();
		var ar = [];
		var res;
		for (var i = 0;
		i < nDocs; i++) {
			doc = this.testDoc(nFields);
			doc._id = i;
			ar.push(doc); 
			if (i % 100000 === 0){
				res = db[colName].insert(ar, {ordered: false});	
				res.counter = i;
				print (JSON.stringify(res));
				ar = [];
			}
		}
		res = db[colName].insert(ar, {ordered: false});
		seconds = this.timeIt(tms);
		return {seconds:seconds, docs_per_second: nDocs/seconds};
	};
	this.testMapReduce = function (colName, nDocs) {
		// usage mt.testMapReduce('del_1000000',1000000)
		// expected results (just for comparison): 
		// results.insert.docs_per_second  ~= 28000, mapReduce.timeMillis" : 10578 
		var res = {};
		res.insert = this.insertTestDocsArr (colName, nDocs, 5);
		res.index = db[colName].ensureIndex({lang: 1});
		res.mapReduce = db.runCommand({mapreduce: colName, 
									   map: function () { emit(this.lang, 1); },
									   reduce: function (key, values) { return Array.sum(values); },
									   out: {replace: 'tmp_mr' }});
		return res;
	};
}
 

function muHelpers() {
	this.dbClients = function (verbose){
		// returns currently connected clients
		var clients = [];
		db.currentOp(true).inprog.forEach(
				  function(op) {
				    if(op.client) {
				    	printjson(op);
				    	connections.push(op.client);
				    }
				  }
				);
		if (verbose) {print (JSON.stringify(clients));}
		return clients;
	}; 
}


function muCollUtils() {
	// to call individual function from mongo shell:
	// db.loadServerScripts();
	// mu = new muCollUtils;
	// mu.functionName(arg ....)
	this.copyField = function  (collectionObj, queryOrWhere, fromFieldNameStr,toFieldNameStr,reportEvery)
	// copies a fromFieldNameStr value to toFieldNameStr for all documents in queryOrWhere
	// call example copyField(db.customers, {_id:{$gt:123}}, 'person.email', person.email_new, 500);
	// warning! use with care, no proper error handling 
	// Parameters:
	// collectionObj, 	a collection object: i.e db.customers
	// queryOrWhere,    a query i.e: {_id:123, order:'yxz'} or a $where js i.e: "this.id==123 && order=='xyz'" 
	// fromFieldNameStr original field name i.e: 'person.email' 
	// toFieldNameStr 	target field name i.e: 'person.email_new'
	// reportEvery		prints document count on reportEvery documents processed 
	{ 
		if (reportEvery === undefined) {reportEvery=1000;}
		if (typeof queryOrWhere == 'string') {
			queryOrWhere={$where:queryOrWhere};
		}
		var docCounter = 0;

		collectionObj.find(queryOrWhere).forEach(
				function(doc) 
				{docCounter +=1;
				var setDict = {};
				setDict[toFieldNameStr] = doc[fromFieldNameStr]; 

				var result=collectionObj.findAndModify({query: {_id:doc._id},  update: {$set: setDict} } );
				if (docCounter % reportEvery === 0) {print( "doc #", docCounter," id:",  doc._id);}  
				} 
		);
		return {docs_processed:docCounter};
	};
	this.extractArrayFld = function (collName, fldName, reportEvery) {
		// extracts an Array field to a new collection named as fldName ;
		// warning! use with care, no proper error handling 
		// Parameters:
		// collName Str	a collection name i.e:  'customers'
		// fldName field name to be extracted i.e.: 'telephones'
		// reportEvery Int reports progress every n
		if (reportEvery === undefined) {reportEvery=100;}  
		var query = {};  
		query[fldName] = {$exists:true}; 
		var counter = {input:0, inserted:0};  
		db[collName].find(query).forEach(
				function(doc) { 
				    counter.input +=1;
					docRatingsArr = doc[fldName];
					var results = db[fldName].insert(docRatingsArr, {ordered: false});
					counter.inserted += results.nInserted;  
					if (counter.input % reportEvery === 0) {print (JSON.stringify(counter));}  
				} 
		);  
		return counter;
	};
	this.removeDupls = function (collectionName, keyField, reportEvery) {
		if (reportEvery === undefined) {reportEvery=10;}  
		sort = {};
		sort[keyField] = 1;
		var myidLast; 
		var res = {docsCnt:0,docsRemoved:0};
		db[collectionName].find().sort(sort).clone().forEach(
			function(doc) {
				    res.docsCnt += 1; 
					if (doc.myid == myidLast) {db[collectionName].remove({_id:doc._id}); res.docsRemoved +=1;}
					else {myidLast = doc.myid;}
				    if (res.docsCnt % reportEvery === 0) {print (JSON.stringify(res))} 
				} 
		);
		return res;
	};
} 


 