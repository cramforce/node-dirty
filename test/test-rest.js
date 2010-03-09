process.mixin(require('./common'));

var dirtier = require('../lib/dirt-tier');

var PORT = 10001;

var server = dirtier.listen(path.dirname(__filename), PORT);

var db = new dirtier.Client("rest.dirty", "127.0.0.1", PORT);

var
  FILE = path.join(path.dirname(__filename), 'dirty.dirty'),
  EXPECTED_FLUSHES = 2,

  TEST_ID = 'my-id',
  TEST_DOC = {hello: 'world'},
  TEST_DOC2 = {another: "doc"},
  TEST_DOC3 = {utf8: "öäüßÖÄÜ"},
  didSetCallback = false,
  didAddCallback = false;

db.set(TEST_ID, TEST_DOC, function(err, doc) {
  assert.ok(!err, JSON.stringify(err, null, " "));
  didSetCallback = true;
  assert.equal(TEST_DOC.hello, doc.hello);
  assert.equal(TEST_ID, TEST_DOC._id);
});

db.get(TEST_ID, function (err, r) {
  assert.ok(!err, JSON.stringify(err, null, " "));
  assert.equal(r.hello, TEST_DOC.hello);
});


db.add(TEST_DOC2, function(err, id, doc) {
  assert.ok(!err, JSON.stringify(err, null, " "));
  didAddCallback = true;
  assert.equal(TEST_DOC2.another, doc.another);
  
  assert.equal(id, doc._id);
  assert.equal(id, TEST_DOC2._id);
});

db.add(TEST_DOC3, function(err, id, doc) {
  assert.ok(!err, JSON.stringify(err, null, " "));
  db.get(id, function (err, doc) {
    assert.ok(!err);
    assert.equal(TEST_DOC3.utf8, doc.utf8);
  
    db.filter(function(doc) {
      return ('another' in doc);
    }, function (err, r) {
      assert.ok(!err, JSON.stringify(err, null, " "));
      assert.ok(r[0].another, TEST_DOC2.another);
      server.close();
    });
  })
});



process.addListener('exit', function() {
  assert.ok(didSetCallback);
  assert.ok(didAddCallback);
});