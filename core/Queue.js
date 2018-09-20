const VoltProcedure = require('voltjs/lib/query');
const { Statement, ANYPARTITION, MULTIPARTITION } = require('./Statement');
const coreModel = require('./Model');
const DAO = require('./DAO');
const { logger } = require('../util');

const coreQueue = function(prefix, client){
	this._procedures = {
		loaders: {
			sp: new VoltProcedure(`${prefix}SPLoader`,['string','array[volttable]']),
			spl: new VoltProcedure(`${prefix}SPLLoader`,['string','array[volttable]','long', 'long']),
			mp: new VoltProcedure(`${prefix}MPLoader`,['array[volttable]']),
			mpl: new VoltProcedure(`${prefix}MPLLoader`,['array[volttable]','long','long']),
		},
		writers: {
			sp: new VoltProcedure(`${prefix}SPWriter`,['string','long','array[volttable]']),
			mp: new VoltProcedure(`${prefix}MPWriter`,['long','array[volttable]'])
		}
	};

	this._client = client;
	this._loadTable = new Map();
	this._execQueue = [];
	this.partitionKey = ANYPARTITION;
};

coreQueue.prototype.queueLoad = function(options){
	let { 
		name, 
		args = {},
		locking = false
	} = options;

	locking = typeof locking !== 'boolean' ? !!locking : locking;

	if ( !name || typeof name !== 'string' ){
		throw new Error('Name must be a valid string');
	}

	const statement = new Statement(name, args);
	const partitionKey = this.getPartitionKeyForValue ( statement.partitionKey() );

	if ( this.partitionKey !== MULTIPARTITION && partitionKey !== null ){
		if (partitionKey === this.partitionKey || this.partitionKey === ANYPARTITION ){
			this.partitionKey = partitionKey;
		} else if ( partitionKey !== ANYPARTITION ){
			this.partitionKey = MULTIPARTITION;
		}
	}

	let volttable = this._loadTable.get(name);
	if( !volttable ){
		volttable = statement.loadOperations();
		this._loadTable.set(name, volttable);
	}

	statement.loadOperation(volttable, locking ? 1 : 0);
};

coreQueue.prototype.queueExec = function(options){
	let { name, args } = options;

	if ( !name || typeof name !== 'string' ){
		throw new Error('queryName must be a valid string');
	}

	const statement = new Statement(name, args);

	const partitionKey = this.getPartitionKeyForValue( statement.partitionKey() ) || MULTIPARTITION;
	if ( this.partitionKey === ANYPARTITION ){
		this.partitionKey = partitionKey;
	} else if ( this.partitionKey !== partitionKey ){
		this.partitionKey = MULTIPARTITION;
	}

	let volttable = null;

	const lastOp = this._execQueue[this._execQueue.length-1];
	if ( lastOp && lastOp.statement === statement.spec.name ){
		volttable = lastOp.volttable;
	} else {
		volttable = statement.execOperations();
		this._execQueue.push({ volttable, statement: statement.spec.name });
	}

	this.partitionKey = ANYPARTITION;
	statement.execOperation(volttable);
};

coreQueue.prototype.load = function(options = {}){

	const { transactionId, lifespan } = options;
	const { loaders } = this._procedures;
  
	let statement = null;
	const args = [];

	const vtList = [];
	for( let entry of this._loadTable ) vtList.push(entry[1]);

	if ( this.partitionKey === ANYPARTITION ){
		this.partitionKey = Math.random().toString().substring(2,7);
	} else if ( this.partitionKey === MULTIPARTITION ){
		this.partitionKey = null;
	}

	const singlePartition = this.partitionKey !== null;
	const locking = !!transactionId && !!lifespan;

	if ( singlePartition ) {
		args.push(this.partitionKey);
	}

	args.push(vtList);

	if ( locking ){
		args.push(transactionId);
		args.push(lifespan);
    
		statement = (singlePartition ? loaders.spl : loaders.mpl).getQuery();
	} else {
		statement = (singlePartition ? loaders.sp : loaders.mp).getQuery();
	}

	statement.setParameters(args);

	let call = null;
	try {
		call = this._client.callProcedure(statement);
	} catch (error){
		vtList.forEach(logger.error);
		throw error;
	}

	call.onQueryAllowed.then( response => {
		this._loadTable.clear();

		return response;
	});

	this.partitionKey = ANYPARTITION;
	return call;
};

coreQueue.prototype.execute = function(transactionId, partitionKey = null){

	const { writers } = this._procedures;
  
	let statement = null;
	const args = [];

	const singlePartition = partitionKey !== null;

	if ( singlePartition ) {
		args.push(partitionKey);
		statement = writers.sp.getQuery();
	} {
		statement = writers.mp.getQuery();
	}

	args.push(transactionId);
	args.push(this._execQueue.map( ({ volttable }) => volttable ));
	statement.setParameters(args);

	let call = null;
	try {
		call = this._client.callProcedure(statement);
	} catch (error){
		args.forEach(logger.error);
		throw error;
	}

	call.onQueryAllowed.then( response => {
		this._execQueue = [];

		return response;
	});

	return call;
};

coreQueue.prototype.getPartitionKeyForValue = function(spec = {}){
	const { type, value } = spec;

	if ( !type || value === null ) return null;
	if ( !this._client._hashinator ) return null;

	return this._client._hashinator.getPartitionKeyForValue(type, value);
};

coreQueue.prototype.getDAO= function(table){
	if (!table) throw new Error('Table must be defined');

	const model = coreModel.getModels().get(table);
	if( ! model ) throw new Error('There is no model defined for table ' + table);

	return new DAO(this, model);
};

module.exports = coreQueue;