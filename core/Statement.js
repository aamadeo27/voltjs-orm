const coreModel = require('./Model');
const VoltTable = require('voltjs/lib/volttable');
const { logger } = require('../util');

const ANYPARTITION = 'ANYPARTITION';
const MULTIPARTITION = 'MULTIPARTITION';

const NOT_GENERATE = 'NOTGENERATE';

const StatementSpec = function(name, sql, table, signature){

	if ( !name || !sql || !signature ) throw new Error('Statement must always have a name, sql and signature');

	this.name = name;
	this.sql = sql;
	this.insert = !! sql.match(/^insert/i);
	this.signature = signature;
	this.model = !!table ? coreModel.getModels().get(table) : null;

	//logger.debug('\n New Statement' + JSON.stringify(this));

	StatementDictionary.add(this);
};

StatementSpec.prototype.rowid = function(args){
	return !!this.model && !this.insert ? this.model.rowid(args) : null;
};

StatementSpec.prototype.partitionKey = function(args){
	return !!this.model ? this.model.getPartitionKey(args) : ANYPARTITION;
};

const StatementDictionary = {
	map: new Map(),

	add(spec){
		this.map.set(spec.name, spec);
	},

	get(name){
		return this.map.get(name);
	}
};

const Statement = function(name, args){
	this.spec = StatementDictionary.get(name);
	this.args = args;

	if( ! this.spec ){
		throw new Error('VoltStatement ' + name + ' is not defined');
	}
};

Statement.prototype.rowid = function(){
	return this.spec.rowid(this.args);
};

Statement.prototype.partitionKey = function(){
	return this.spec.partitionKey(this.args);
};

Statement.prototype.loadOperations = function(){
	const operations = new VoltTable();

	operations.addColumn('STATEMENT','string');
	operations.addColumn('PRIMARY_KEY_SPEC','string');
	operations.addColumn('TABLE','string');
	operations.addColumn('LOCKING','smallint');

	this.spec.signature.forEach( param => operations.addColumn(param.name, param.type));

	return operations;
};

Statement.prototype.loadOperation = function(operations, locking = 0){
	let pkIndexes = this.spec.model.pkIndexes().join(',');
	let args = [this.spec.name, pkIndexes , this.spec.model.table, locking];

	args = args.concat(this.spec.signature.map( param => this.args[param.name] ));

	operations.addRow(args);
};


Statement.prototype.execOperations = function(){
	const operations = new VoltTable();

	operations.addColumn('STATEMENT','string');
	operations.addColumn('ROWID','long');

	this.spec.signature.forEach( param => operations.addColumn(param.name, param.type));

	return operations;
};

Statement.prototype.execOperation = function(operations){
	let args = [this.spec.name, this.rowid()];
	args = args.concat( this.spec.signature.map( param => this.args[param.name] ));

	operations.addRow(args);
};

//PreDefined Statements
StatementDictionary.add(new StatementSpec('UPDATE_TRIGGERS', NOT_GENERATE, null, []));

module.exports = {
	Statement,
	publish: (name, sql, table, signature) => new StatementSpec(name, sql, table, signature),
	getStatements: () => StatementDictionary.map,
	ANYPARTITION,
	MULTIPARTITION,
	NOT_GENERATE
};