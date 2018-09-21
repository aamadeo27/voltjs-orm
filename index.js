const Generator = require('./gen/Generator');
const core = require('./core');
const util = require('./util');

module.exports = {
	Generator,
	...core,
	genId: util.uuid
};