/**
 * useful js functions to be used by map-reduce 
 * functions should start on column 0 of a line and end with '}' in column 0 of a line
 * W A R N I N G : remember js can't handle large integer fields >53bits although mongo can
 * for debug use print and printjson (output will print appear in the logs if verbosity is high enough) 
 * i.e: ('foo',JSON.stringify({foo:bar));  
 */

function GroupCountsMap () {
	emit(this.%s, 1);
}

function GroupCountsReduce (key, values) {
	return Array.sum(values); 
} 

function OrphansMap () {  
	//	finds how many documents exists with given field in each collection 
	//	example scope= {'phase':1}  phase =1 on first Map call, =2 on second 
	var emkey= this.%s;  // %s   it will be replaced by caller with exact field name
	var vl;
	if (phase==1) {vl = {a:1,b:0,sum:1};}  
	else          {vl = {a:0,b:1,sum:1};} 
	emit(emkey, vl);
} 

function OrphansReduce(key, values) {
	var result={a:0,b:0,sum:0}; 
	for(var i in values) {
		result.a += values[i].a;  
		result.b += values[i].b; 
		result.sum += values[i].sum;
	}  
	return result;
}  

function JoinMap () {  
	//	joins 2 collections on a given field
	var emkey= this.%s;  // %s   it will be replaced by caller with exact field name
	var vl;
	if (phase==1) {vl={a:this,b:null};}  
	else          {vl={a:null,b:this};} 
	emit(emkey, vl);
} 

function JoinReduce(key, values) {
	var result={a:null,b:null}; 
	for(var i in values) { 
		result.a = values[i].a;  
		result.b = values[i].b;  
	}  
	return result;
}


function KeysMap() {      
	var tmp,tmpEmpty,ChildObjTp,ChildIsAr; 
	var levelCurrent=0; 
	var record=this;
	// function isArray(obj) {return typeof(obj)=='object'&&(obj instanceof Array);}   
	// function emptyIf(obj){if (typeof(this[obj])=='function')   {return  ' ';} else {return obj+' ';} }  //@note date fields return .tojson so strip it 
	function StrHash (str) {
		var hash = 0, i, chr, len;
		if (str.length === 0) return hash;
		for (i = 0, len = str.length; i < len; i++) {
		  chr   = str.charCodeAt(i);
		  hash  = ((hash << 5) - hash) + chr;
		  hash |= 0; // Convert to 32bit int
		}
		return hash;
	}
	function keysToArray(obj,propArr,levelMax, _level) {
		/**	example: r1=keysToArray(doc,[null,[] ],2,0) 
		 	_level is used for recursion and should always called with 0
		 	if levelMax is negative returns maximum level
		 	levelMax=0 means top level only 2 up to 2nd depth level etc.
		*/ 
			for (var key in obj) { 
				if (obj.hasOwnProperty(key)) { 
					if (obj[key] instanceof Object && ! (obj[key]  instanceof Array))  
						if (levelMax < 0 || _level+1  <= levelMax) { 
							{propArr[1].push(keysToArray(obj[key],[key,[] ],levelMax,_level+1));}
						}
						else {}  //needed coz nested if ?  
					{propArr[1].push(key);} 
				}
			} 
			return  propArr;
		} 
	//----------------------------------------------------------------------------------------------
	function arrayToStr(lst,prevKey,delimiter, inclKeys,levelMax,_level,_levelMaxFound) {
		/**	example: r2=arrayToStr(r1,'','|',true,2,0,0)
		 	_level and _levelMaxFound is used for recursion and should always called with value 0
		 	if levelMax is negative returns maximum level
		 	levelMax=0 means top level only 2 up to 2nd depth level etc.
		*/ 
			var rt,i;
			_levelMaxFound=Math.max(_level,_levelMaxFound);
			if (prevKey !=='') {prevKey += '.';}
			var rtStr ='';  
			if (lst[0])		{prevKey += lst[0]+'.';} 
			if (inclKeys)	{rtStr += prevKey.slice(0,-1);} 
			for (var n in lst[1]) {
				i=lst[1][n];
				if (typeof(i)=='string') {
					rtStr += delimiter + prevKey + i;
				}
				else
				{
					if (levelMax < 0 || _level+1  <= levelMax) {
						rt=arrayToStr(i,prevKey.slice(0,-1),delimiter, inclKeys,levelMax,_level+1,_levelMaxFound);
						rtStr += delimiter + rt[0];
						_levelMaxFound=Math.max(rt[1],_levelMaxFound);
					}
					else {} 
				}
			}
			if (rtStr[0] == delimiter) {rtStr=rtStr.slice(1);}  // Lstrip delimiters if any
			return [rtStr,_levelMaxFound];
		}
	//----------------------------------------------------------------------------------------------
	var keysV = keysToArray(this,[null,[] ] ,parms.levelMax, 0);   // we can't sort here coz array is nested
	keysV = arrayToStr(keysV,'',' ', parms.inclHeaderKeys,-1,0,0); 
	var MaxDepth=keysV[1];
	keysV = keysV[0].split(' ');	// so we can sort 
	keysV.sort();				// sort to make sure identical records map to same id  
	emit (StrHash(keysV.join(' ')),  {cnt:1, percent:0.0,depth:MaxDepth,fields:keysV});
	// we emit a hash for id to get around the limitation of max 1028 Bytes indexed field of mongoDB
	// this can lead to wrong results because of possible hash collisions but risk is negligible
	// in our case and any way doesn't affect much
}

//function del_KeysReduce (key, values) {
//	return Array.sum(values);
//}

function KeysReduce (key, values) {
    var total = {cnt:0, percent:0.0,depth:values[0].depth,fields:values[0].fields};
    for(var i in values) {
    	total.cnt += values[i].cnt; 
    	} 
    return total;
} 	

function KeysMetaMap() {   
  fields=this.value.fields;
  value=this.value;
  var fldname;
  for (var fld in fields) {
	  fldname=fields[fld];
	  depth=(fldname.match(/\./g)||[]).length+1;
	  emit(fldname,{cnt:value.cnt, percent:value.percent,depth:depth});
   } 
}

function KeysMetaReduce (key, values) { 
    var total = {cnt:0,percent:0.0,depth:values[0].depth};  
    for(var i in values) {
    	total.cnt += values[i].cnt;
    	total.percent += values[i].percent; 
    	}
    return total;
}

 

 