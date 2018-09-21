const path = require('path');
const fs = require('fs');
const { RESULT_STATUS } = require('voltjs/lib/voltconstants');
const { Parser } = require('voltjs/lib/parser');
const uuid = require('uuid/v1');

const _mkdir = (base, dirs, resolve, reject) => {
	const dir = base + path.sep + dirs.splice(0,1)[0];

	fs.mkdir(dir, error => {
		if ( error ) reject(error);
		else if (dirs.length === 0 || dirs[0].length === 0) resolve();
		else _mkdir(dir, dirs, resolve, reject);
	});
};

const mkdir = dirpath => new Promise( (resolve, reject) => {
	let dirs = dirpath.split(path.sep);

	let i = 0;
	for(; i < dirs.length; i++ ){
		let parentdir = dirs.slice(0,i+1).join(path.sep);
		
		try {
			if ( !fs.statSync(parentdir).isDirectory() ) reject(parentdir + ' exists but is not a directory.');
		} catch(error){
			break;
		}
	}

	if ( i == dirs.length ) return resolve();

	process.stdout.write('Creating directory ' + dirpath + '\n');
	_mkdir( dirs.slice(0,i).join(path.sep), dirs.slice(i, dirs.length) , resolve, reject);
});

const execStatement = async (stmt, description) => {
	const response = await stmt.read;
	
	if ( response.code || response.results.status !== RESULT_STATUS.SUCCESS ){
		throw new Error(response.results.statusString);
	} else if ( description ){
		const status = response.results.statusString;
		process.stdout.write( blue(description) + ' ' + green(status) + '\n');
	}

	return response;
};

const red = str =>  '\x1b[31m' + str + '\x1b[37m';
const green = str => '\x1b[32m' + str + '\x1b[37m';
const blue = str => '\x1b[34m' + str + '\x1b[37m';
const yellow = str => '\x1b[33m' + str + '\x1b[37m';


const print = param => typeof param === 'string' ? param : (param instanceof Error ? param.toString() : JSON.stringify(param, undefined, ' '));
const logger = {
	info: str => process.stdout.write(green(print(str)) + '\n'),
	profile: str => process.stdout.write(yellow(print(str)) + '\n'),
	error: str => process.stderr.write(red(print(str)) + '\n'),
	debug: str => !!logger.debugMode && process.stdout.write(blue(print(str)) + '\n'),
	debugMode: false
};

const Profiler = function(){
	this.list = [];
};

Profiler.prototype.set = function(event){

	if ( this.list.length > 0 && this.list[ this.list.length - 1].event === 'Done' ) this.list = [];

	this.list.push({ event, time: new Date().getTime() });
};

Profiler.prototype.show = function(){
	let table = '';

	for(let i = 0; i < this.list.length ; i++){
		let data = this.list[i];
		let delta = ( (i+1) < this.list.length) ? (this.list[i+1].time - data.time) + 'ms' : '';
		table += `${data.event}\t${delta}\n`;
	}

	logger.profile(table);
};

const buffer = new Buffer(16);
const parser = new Parser(buffer);
const genUUID = () => {
	uuid({},buffer,0);
	parser.position = 0;

	return parser.readLong();
}

module.exports = {
	mkdir,
	execStatement,
	logger,
	uuid: genUUID,
	profiler: new Profiler()
};