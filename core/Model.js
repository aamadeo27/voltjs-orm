const hash = require('murmurhash-native').murmurHash128x64;
const Message = require('voltjs/lib/message').Message;

let coreStatement = null;
const publishStatements = (model) => {
	coreStatement = coreStatement || require('./Statement');
	
	let params = '?' + ', ?'.repeat(model.schema.length -1);
	let name = model.table + '_INSERT';
	let sql = `INSERT INTO ${model.table} values (${params})`;
	coreStatement.publish(name, sql, model.table, model.schema);

	if ( !model.pk ) return null;

	const pk = model.pkIndexes().map( i => model.schema[i]);
	const pkCondition = pk.reduce( (c,col,i) => c + `${ i > 0 ? ' AND ' : '' }${col.name} = ?` ,'');

	name = model.table + '_LOAD';
	sql = `SELECT * FROM ${model.table} WHERE ${pkCondition}`;
	coreStatement.publish(name, sql, model.table, pk);

	name = model.table + '_DELETE';
	sql = `DELETE FROM ${model.table} WHERE ${pkCondition}`;
	coreStatement.publish(name, sql, model.table, pk);

	let signature = model.schema.filter( e => ! model.isPk.has(e.name) );
	params = signature.reduce( (c,col,i) => c + `${ i > 0 ? ', ' : '' }${col.name} = ?` ,'');
	name = name = model.table + '_UPDATE';
	sql = `UPDATE ${model.table} SET ${params} WHERE ${pkCondition}`;
	signature = signature.concat(pk);
	coreStatement.publish(name, sql, model.table, signature);
};

const VoltModel = function(table, schema, primaryKey = '', partitionKey = null){
	this.table = table;
	this.partitionKey = partitionKey;
	this.schema = schema;
	this.pk = primaryKey;
	this.isPk = new Set();

	this.pk.split(',').forEach( col => this.isPk.add(col) );

	ModelsMap.add(this);
	publishStatements(this);
};

VoltModel.prototype.pkIndexes = function(){
	return this.schema.map( (c,i) => this.isPk.has(c.name) ? i : null ).filter(e => e !== null );
};

VoltModel.prototype.types  = function(){
	return this.schema.map( ({ type }) => type );
};

VoltModel.prototype.getPartitionKey  = function(args){
	return this.partitionKey !== null ? { 
		value: args[this.partitionKey], 
		type: this.schema.find(c => c.name === this.partitionKey).type 
	} : null;
};

const view = new DataView( new ArrayBuffer(8));
const parser = new Message();
VoltModel.prototype.rowid = function(args){
	if ( !args ) return null;

	const cols = this.pk.split(',');
	
	let str = cols.reduce( 
		(r,col) => r + '.' + ( args[col] !== undefined ? args[col].toString() : 'null'), 
		this.table
	);

	let buffer = Buffer.from(str.substr(0,str.length/2));
	let hashed = hash(buffer, 0, buffer.length, 0);
	view.setUint32(0, parseInt( hashed.substring(0,8), 16) );

	buffer = Buffer.from(str.substr(str.length/2));
	hashed = hash(buffer, 0, buffer.length, 0);
	view.setUint32(4, parseInt( hashed.substring(0,8), 16) );

	parser.position = 0;
	parser.buffer = Buffer.from(view.buffer);

	const rowid = parser.readLong();

	return rowid;
};

const ModelsMap = {
	models: new Map(),

	add(model){
		if ( this.models.get(model.table) ){
			console.warn('Overriding model for table: ' + model.table);
		}

		this.models.set(model.table, model);
	},

	get(table){
		return this.models.get(table);
	}
};

module.exports = {
	publish: (table, schema, primaryKey, partitionKey) => new VoltModel(table, schema, primaryKey, partitionKey),
	getModels: () => ModelsMap.models
};