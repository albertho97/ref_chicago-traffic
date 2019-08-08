var http = require('http')
  , connect = require('connect')
  , serveStatic = require('serve-static');
  
  var admin = require('firebase-admin');
  admin.initializeApp({
    credential: admin.credential.applicationDefault()
  }); 
  var db = admin.firestore();
 
function lookupname(id)
{
    switch(id)
    {
      case "1000":
        return "crawford";
      case "1001":
        return "kedzie"
      case "1002":
        return "california"
      case "1003":
        return "western"
      case "1":
        return "crawford"
    }
}

var unsubscribe = db.collection("segment_event").onSnapshot(function (querySnaphot) {
  // do something with the data.
});

function listenDiffs(db, done, res) {
  // [START listen_diffs]
  var observer = db.collection('segment_event').where('segment_id', '>=', '1000').where('segment_id', '<=', '1003')
  .onSnapshot(querySnapshot => {
    querySnapshot.docChanges().forEach(change => {
      if (change.type === 'added') {
        d = change.doc.data()
        localtime = Date.parse(d['time'])
        message = '{"segment": "' + lookupname(d["segment_id"]) + '", "speed": "' + d["speed"] + '", "time": "' + localtime + '"}'
        res.json(message)          
        console.log("New : " + message)        
      }
      if (change.type === 'modified') {
        d = change.doc.data()
        localtime = Date.parse(d['time'])
        message = '{"segment": "' + lookupname(d["segment_id"]) + '", "speed": "' + d["speed"] + '", "time": "' + localtime + '"}'
        res.json(message)  
        console.log("Modified : " + message)        
      }
      if (change.type === 'removed') {
        //console.log('Removed segment event: ', change.doc.data());
      }
    });
    // [START_EXCLUDE silent]
    observer();
    done()
    // [END_EXCLUDE]    
  });
}

function ticker(req, res) {
  req.socket.setTimeout(Number.MAX_VALUE);

  // segment_id to name
  // 1000, Crawford
  // 1001, Kedzie
  // 1002, California
  // 1003, Western
  // message = '{"segment": "crawford", "speed": "24", "time": "2019-03-03 04:01:07.000000"}'
  // res.json(message)
  
  //send headers for event-stream connection
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });

  res.json = function (obj) { res.write("data: " + obj + "\n\n"); }
  res.json(JSON.stringify({}));

  // streamDocument(db, function () {})
  listenDiffs(db, function () {}, res)

  // The 'close' event is fired when a user closes their browser window.
  req.on("close", function () {
    unsubscribe();
  });
}

connect()
  .use(serveStatic(__dirname))
  .use(function (req, res) {
    if (req.url == '/eventCounters') {
      ticker(req, res);
    }
  })
  .listen(80);