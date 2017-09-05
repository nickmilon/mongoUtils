/**
 * 
 */
function InsertTestDocs (colName, nDocs, wc) {
	db[colName].drop()
    var before = new Date();
	for (var i = 0;
	i < nDocs; i++)
	{db[colName].insert({'_id':i, 'foo':i}, writeConcern={w: wc})
	};
        var after = new Date();
        var execution_mills = after - before;
        var docsPerSecond = nDocs / (execution_mills / 1000); 
        print ('dt start', before, 'dt end', after)
        print ('ms', execution_mills, 'docsPerSecond', docsPerSecond);
}

function FetchDocs (colName, nDocs) {
	var before = new Date();
	for (var i = 0;
	i < nDocs; i++)
	{result = db[colName].findOne({'_id':i});
	// print(JSON.stringify(result))
	};
        var after = new Date();
        var execution_mills = after - before;
        var docsPerSecond = nDocs / (execution_mills / 1000); 
        print ('dt start', before, 'dt end', after)
        print ('ms', execution_mills, 'docsPerSecond', docsPerSecond);
}