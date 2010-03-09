var sys  = require('sys'), 
   Path  = require('path'),
   http  = require('http'),
   URL = require('url'),
   Dirty = require('./dirty').Dirty,
   crc32 = require("./crc32");

var files = {};

exports.listen = function (path, port) {
  var server = http.createServer(function (req, res) {
    var url = URL.parse(req.url, true);
    
    var pathInfo = url.pathname.match(/^\/([^\/]+)\/*([^\/]*)/)
    
    if(pathInfo) {
      var filename = decodeURIComponent(pathInfo[1]);
      var id       = decodeURIComponent(pathInfo[2]);
      
      filename = Path.join(Path.dirname(path), filename);
      
      var body = "";
      req.addListener("data", function (data) {
        body += data;
      });
      req.addListener("end", function () {

        var dirt = files[filename];
        if(!dirt) {
          dirt = new Dirty(filename);
          files[filename] = dirt
        }
        
        if(dirt.loaded) {
          handleRequest({
            req: req,
            res: res, 
            dirt: dirt, 
            filename: filename, 
            id: id, 
            body: body,
            url: url
          });
        } else {
          dirt.addListener("load", function (err) {
            dirt.loaded = true;
            if(err) {
              sendError(err)
            } else {
              handleRequest({
                req: req,
                res: res, 
                dirt: dirt, 
                filename: filename, 
                id: id, 
                body: body,
                url: url
              });
            }
          });
        }
      });
    } else {
      sendBadRequest(res);
    }
  });
  server.listen(port);
  return server;
}

function sendError(res, err) {
  res.writeHead(500, {'Content-Type': 'application/json'});
  res.write(JSON.stringify(err));
  res.close();
}

function sendBadRequest(res) {
  res.writeHead(400, {'Content-Type': 'text/plain'});
  res.close();
}

var actions = {
  add: function (info) {
    var uuid = info.dirt.add(JSON.parse(info.body));
    return uuid;
  },
  set: function (info) {
    info.dirt.set(info.id, JSON.parse(info.body));
  },
  get: function (info) {
    var ret = info.dirt.get(info.id);
    return ret;
  },
  filter: function (info) {
    var filterStr = info.url.query.filter;
    var filter = filterStr ? eval("var __func__ = "+filterStr+";__func__;") : function () { return true };
    return info.dirt.filter(filter);
  },
  remove: function (info) {
    info.dirt.remove(info.id)
  }
};

function handleRequest(info) {
  var action = "filter";
  var method = info.req.method;
  if(method === "GET" && info.id) {
    action = "get"
  }
  else if(method === "POST") {
    if(info.id) {
      action = "set";
    } else {
      action = "add"
    }
  }
  else if(method === "DELETE") {
    action = "remove"
  }
  else if(method !== "GET") {
    sendBadRequest(info.res);
  }
  var a = actions[action];
  if(!a) {
    return sendError(info.res, "Unknown action "+action);
  }
  var ret;
  try {
    ret = a(info);
  } catch(e) {
    return sendError(info.res, e);
  }
  info.res.writeHead(200, {'Content-Type': 'application/json'});
  if(ret) {
    info.res.write(JSON.stringify(ret), "utf8");
  }
  info.res.close();
}

exports.Client = function (filename, host, port) {
  this.filename = filename;
  this.client = http.createClient(port, host);
  this.host = host;
}

exports.Client.prototype = {
  
  request: function (method, id, data, cb) {
    var path = "/"+encodeURIComponent(this.filename);
    if(id) {
      path += "/"+encodeURIComponent(id);
    }
    if(method === "GET" && data) {
      path += "?";
      var first = true;
      for(var i in data) {
        if(!first) {
          path += "&";
        }
        path += encodeURIComponent(i)+"="+encodeURIComponent(data[i])
        first = false;
      }
      data = null;
    }
    var req = this.client.request(method, path, {
      host: this.host
    });
    if(data) {
      req.write(data, "utf8");
    }
    req.addListener('response', function (response) {
      if(response.statusCode === 200) {
        response.setBodyEncoding("utf8");
        var body = "";
        response.addListener("data", function (chunk) {
          body += chunk;
        })
        response.addListener("end", function () {
          cb(null, body);
        })
      } else {
        cb(new Error("Error "+response.statusCode))
      }
    })
    req.close();
  },
  
  add: function (data, cb) {
    this.request("POST", null, JSON.stringify(data), function (err, ret) {
      if(err) return cb(err);
      var id = JSON.parse(ret);
      data._id = id;
      cb(null, id, data);
    })
  },
  
  set: function (id, data, cb) {
    this.request("POST", id, JSON.stringify(data), function (err) {
      if(err) return cb(err);
      data._id = id;
      cb(null, data);
    })
  },
  
  get: function (id, cb) {
    this.request("GET", id, null, function (err, data) {
      if(err) return cb(err);
      var obj = JSON.parse(data);
      cb(null, obj);
    })
  },
  
  remove: function (id, cb) {
    this.request("DELETE", id, null, function (err, data) {
      if(err) return cb(err);
      cb(null, id);
    })
  },
  
  filter: function (filter, cb) {
    var filterStr = filter ? filter.toString() : ""
    this.request("GET", null, { filter: filterStr }, function (err, data) {
      if(err) return cb(err);
      var array = JSON.parse(data);
      cb(null, array);
    })
  }
}

var makeShardingFunction = function (len) {
  return function (id) {
    return crc32.hash(id) % len;
  };
}

exports.shard = function (db, mappingArray, shardingFn) {
  if(!shardingFn) {
    shardingFn = makeShardingFunction(mappingArray.length)
  }
  db.filter(function () { return true }).forEach(function (doc) {
    var shard = shardingFn(doc._id);
    var destination = mapping[shard];
    destination.set(doc._id, doc);
  });
}