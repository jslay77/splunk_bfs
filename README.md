# splunk_bfs

# Sample SPL for testing

 | makeresults
 | eval data="AM,BU;AK,BT;BU,CY;BT,CX;CY,D0;CY,DZ;CX,D9;CX,D8;CX,D1;CX,D0;CX,D2"
 | makemv data delim=";"
 | mvexpand data
 | makemv data delim=","
 | eval Parent=mvindex(data,0),Child=mvindex(data,1)
 | bfs parent=Parent child=Child
