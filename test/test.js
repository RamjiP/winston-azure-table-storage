/*!
 * winston-azure-table-storage
 * Copyright(c) 2017 Ramji Piramanayagam
 * Apache 2.0 Licensed
 */

var should = require('chai').should;

var fs = require('fs');
var testCredentials = JSON.parse(fs.readFileSync('./test/credentials.json', 'utf8'));
var tableService = require('azure-storage').createTableService(testCredentials.account, testCredentials.key);

var winston = require('winston');

require('../lib/winston-azure-table-storage');

var logger = winston.createLogger({
    transports: [
        new (winston.transports.AzureTable)({
            account: testCredentials.account,
            key: testCredentials.key,
            table: testCredentials.table,
            partition: require('os').hostname(),
            level: 'warn',
            metaAsColumns: true
        })
    ]
});


describe('winston-azure:', function () {

    before(function (done) {
        console.log(logger.transports);
        logger.transports[0].createTableIfNotExists(function (err) {
            return done(err);
        });
    });

    after(function(done) {
        logger.transports[0].explore(1, function (result) {
            var entity = result.entries[0];

            return done(entity === null ? 'Entity not found in table!' : null);
        });
    });

    describe('a logger', function () {

        it('should log to Azure', function (done) {

            logger.warn('Warning, you are logging to Azure',
                {uno: 1, dos: 'two', tres: true},
                function (err, level, msg, meta) {
                    should.not.exist(err);
                    level.should.equal('warn');
                    msg.should.equal('Warning, you are logging to Azure');


                    tableService.queryEntities(testCredentials.table, null, null, function (error, result) {

                        setTimeout(function () {
                            done();
                        }, 75);
                    });


                });
        });
    });
});