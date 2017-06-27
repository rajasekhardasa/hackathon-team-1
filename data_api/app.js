'use strict'

var MongoClient = require('mongodb').MongoClient;

let atlas_connection_uri;
let cachedDb = null;

exports.handler = (event, context, callback) => {
    var uri = process.env['MONGODB_ATLAS_CLUSTER_URI'];

    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? JSON.stringify(err): JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        }
    });

    if (atlas_connection_uri != null) {
        processEvent(event, context, done);
    } else {
        atlas_connection_uri = uri;
        processEvent(event, context, done);
    }
};

function processEvent(event, context, done) {

    context.callbackWaitsForEmptyEventLoop = false;
    var params = event.queryStringParameters;
    if (event.httpMethod != 'GET') {
        done({error: "Only 'GET' is supported!"}, null);
    }

    if (params == null || params.from == null || params.to == null) {
        done({error: '"from" and "to" query parameters are required!'}, null);
    }

    try {
        if (cachedDb == null) {
            console.log('Connecting to database');
            MongoClient.connect(atlas_connection_uri, function(err, db) {
                cachedDb = db;
                return queryBankActivity(cachedDb, params, done);
            });
        } else {
            queryBankActivity(cachedDb, params, done);
        }
    } catch (err) {
        console.log('Something went wrong with DB connection');
        console.log(err);
        done({message: 'Technical Error'}, null);
    }


}

function queryBankActivity(db, params, done) {
    var bankActivityCollection = db.collection("bank_activity");
    console.log(params);
    var query = buildQuery(params);
    console.log(query);
    bankActivityCollection.find(query).toArray((err, result) => {
        if (err) {
            done(err, null);
        } else {
            result.forEach((activity) => {
                delete activity["_id"];
            });

            console.log(result);
            done(null, result);
        }
    });
}

function buildQuery(params) {
    console.log(params);
    var query = {};
    var from = new Date(params.from);
    var to = new Date(params.to);
    query['meta.date'] = {
        '$gte': from,
        '$lte': to
    };
    
    if (params.category) {
        query['meta.category'] = params.category;
    }
    return query;
}