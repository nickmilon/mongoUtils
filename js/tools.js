/**
 * 
 */

function TestDoc (n) {
	// produces a test doc with n+1 fields containing random strings
	var doc={}; 
	doc['lang']=['da', 'de', 'en', 'es', 'fi', 'fr', 'hu', 'it', 'nl', 'pt', 'ro', 'ru', 'sv', 'tr'][Math.floor(Math.random() * 13)]
	for (var i = 0;
	i < n; i++) { doc['fld_'+i]=Math.random().toString(34).slice(2)} 
	return doc;
}

function InsertTestDocs (colName, nDocs, nFields) { 
	for (var i = 0;
	i < nNocs; i++) { doc=TestDoc(nFields); doc['_id']=i;
	db[colName].insert(doc)
	}
}

function StrHash (str) {
	  var hash = 0, i, chr, len;
	  if (str.length == 0) return hash;
	  for (i = 0, len = str.length; i < len; i++) {
	    chr   = str.charCodeAt(i);
	    hash  = ((hash << 5) - hash) + chr;
	    hash |= 0; // Convert to 32bit integer
	  }
	  return hash;
};

function StrHash (str) {
var hash = 0, i, chr, len;
if (str.length == 0) return hash;
for (i = 0, len = str.length; i < len; i++) {
  chr   = str.charCodeAt(i);
  hash  = ((hash << 5) - hash) + chr;
  hash |= 0; // Convert to 32bit integer
}
return hash;
};