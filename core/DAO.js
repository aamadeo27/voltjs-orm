const { logger } = require('../util');

const DAO = function(queue, model){
	this.queue = queue;
	this.model = model;
};

DAO.prototype.queueSelectForUpdate = function(...argList){
	const name = this.model.table + '_LOAD';
	const args = {};

	this.model.pk.split(',').forEach( (col, i) => args[col] = argList[i] );

	this.queue.queueLoad({ name, args, locking: true });
};

DAO.prototype.queueSelect = function(...argList){
	const name = this.model.table + '_LOAD';
	const args = {};

	this.model.schema.forEach( (col, i) => args[col.name] = argList[i] );
	this.queue.queueLoad({ name, args, locking: false });
};

DAO.prototype.load = function(transactionId = null, lifespan = 0 ){
	return this.queue.load({ transactionId, lifespan });
};

DAO.prototype.insert = function(entity){
	this.queue.queueExec({ args: entity, name: this.model.table + '_INSERT' });
};

DAO.prototype.update = function(entity){
	this.queue.queueExec({ args: entity, name: this.model.table + '_UPDATE' });
};

DAO.prototype.delete = function(entity){
	this.queue.queueExec({ args: entity, name: this.model.table + '_DELETE' });
};

module.exports = DAO;