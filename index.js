var http = require('http');
var fs = require('fs');
var path = require('path');

var csv = require('ya-csv');
var qiniu = require('qiniu');
var async = require('async');



var PIC_CSV_FILE = "pics.csv";
var QINIU_CONFIG_FILE = '.qiniu';
var QINIU_PUT_LIMIT = 10;
var PREFIX_KEY = 'school/asset/';

var qiniuConfig = JSON.parse(fs.readFileSync(QINIU_CONFIG_FILE, 'utf8'));

var csvLines = [];
var qiniuPics = [];

var reader = csv.createCsvFileReader(PIC_CSV_FILE, {
    columnsFromHeader: true
});

reader.addListener('data', function (data) {
    csvLines.push(data);
});

reader.addListener('end', function () {

    var size = csvLines.length;
    console.log('Analysis csv file, there are ' + size + ' files need upload');

    async.mapLimit(csvLines, QINIU_PUT_LIMIT, function (data, callback) {

        putFile(data, callback);

    }, function (err, result) {
        fs.writeFile('err.log', err && err.toString());
        fs.writeFile('result.json', JSON.stringify(result));
    })

})


qiniu.conf.ACCESS_KEY = qiniuConfig.AK;
qiniu.conf.SECRET_KEY = qiniuConfig.SK;

var policy = uptoken(qiniuConfig.bucket);

function putFile(data, callback) {

    var imgUrl = data.picture_url;
    var id = data.id;
    var schoolId = data.school_id;

    http.get(imgUrl, function (res) {

        var imgBuffers = [];

        res.on("data", function (chunk) {
            imgBuffers.push(chunk);
        });

        res.on('end', function () {
            qiniu.io.put(policy, PREFIX_KEY + path.basename(imgUrl), Buffer.concat(imgBuffers), null, function (err, ret) {
                if (!err) {
                    callback(null, {
                        'id': id,
                        'school_id': schoolId,
                        'key': ret.key,
                        'hash': ret.hash
                    });
                } else {
                    // http://developer.qiniu.com/docs/v6/api/reference/codes.html
                    callback(err);
                }
            });
        });

    }).on('error', function (e) {
        console.log(e)
    });
}


function uptoken(bucketname) {
    var putPolicy = new qiniu.rs.PutPolicy(bucketname);
    return putPolicy.token();
}