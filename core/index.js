const statement = require('./Statement');
const model = require('./Model');
const Queue = require('./Queue');

module.exports = {
	publishStatement: (name, sql, table, signature) => statement.publish(name, sql, table, signature),
	getStatements: () => statement.getStatements(),

	publishModel: (table, schema, primaryKey, partitionKey) => model.publish(table, schema, primaryKey, partitionKey),
	getModels: () => model.getModels(),

	constants: {
		ANYPARTITION: statement.ANYPARTITION,
		MULTIPARTITION: statement.MULTIPARTITION,
		NOT_GENERATE: statement.NOT_GENERATE
	},

	Queue
};