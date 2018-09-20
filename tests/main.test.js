
const VoltClient = require('voltjs/lib/client');
const { LOGIN_ERRORS } = require('voltjs/lib/voltconstants');
const core = require('../core');
const { execStatement, logger, profiler } = require('../util');
const config = require('./config');
logger.debugMode = true;

require('./user');
require('./item');

const client = new VoltClient([config]);
let coreQueue = null;
let dao = null;

const ids = [0,1,2,7,11,15,19,23];
const userInsert = id => `insert into users values( ${id}, 'user-${id}', now, '{}' );\n`;
const itemInsert = id => `insert into items values( '${id}', 'group-${id}', 'Item_${id}','Item n° ${id}', dateadd(year,-1,now), dateadd(year,1,now) );\n`;

const now = new Date().getTime();
const user = id => ({
	ID: id,
	NAME: 'user-' + id,
	LAST_LOGIN: new Date(now - 3600 * 960),
	USER_SETTINGS: '{ "status": "mix-insert" }'
});

const item = id => ({
	ID: '' + id,
	GROUP_ID: 'group-' + id/2,
	NAME: 'item-' + id,
	DESCRIPTION: 'item number ' + id,
	VALID_SINCE: new Date(now - 3600 * 960),
	VALID_UNTIL: new Date(now + 3600 * 960)
});

const MIX_VB_STATEMENT = [
	{ statement: 'USERS_UPDATE', args: user( ids[0] ) },
	{ statement: 'USERS_UPDATE', args: user( ids[7] ) },
	{ statement: 'USERS_INSERT', args: user(30) },
	{ statement: 'ITEMS_INSERT', args: item(30) },
	{ statement: 'USERS_INSERT', args: user(31) },
	{ statement: 'ITEMS_INSERT', args: item(31) },
	{ statement: 'ITEMS_DELETE', args: { ID: ids[1] + '' } },
	{ statement: 'ITEMS_DELETE', args: { ID: ids[6] + '' } },	
	{ statement: 'USERS_DELETE', args: { ID: ids[2] } },
	{ statement: 'USERS_DELETE', args: { ID: ids[5] } }
];

/**
 * Prepare.
 * 	Connect
 * 	Drop Procedures
 *  Drop Table
 *  Create Table
 *  Partition Table
 *  Create Procedures
 *  Insert Data
 */
beforeAll( async () => {
	const { connected, errors } = await client.connect();
  
	if (!connected){
		throw new Error('Connection Errors' + errors.map(e => LOGIN_ERRORS[e]));
	}

	let initSQL = 'delete from users; delete from items;\n';
	initSQL +=  ids.map( id => userInsert(id) + itemInsert(id) ).join('');
	execStatement(client.adHoc(initSQL));
	
	logger.info('Data Initialized');
	
	coreQueue = new core.Queue('Example',client);
	dao = { 
		users: coreQueue.getDAO('USERS'),
		items: coreQueue.getDAO('ITEMS')
	};

	await new Promise( (resolve, reject) =>{
		let iterations = 0;
		let handle = setInterval( () => {
			if ( client._hashinator ){
				clearInterval(handle);
				resolve();
			} else if ( ++iterations === 10 ){
				clearInterval(handle);
				reject('No hashinator defined');
			}
		}, 1000);
	});

}, 60000);

afterAll( () => {
	logger.info('Closing Client');
	client.exit();
});

//*
test('Loaders', async () => {
	jest.setTimeout(8 * ids.length);
	for(let i = 0 ; i < ids.length; i++){
		dao.users.queueSelectForUpdate(ids[i]);
		const response = await execStatement(dao.users.load(1,2));

		if ( response.results.table[0].data.length === 0 ) throw new Error('LOAD-LOCK: No record found with id: ' + ids[i]);
	}
});
// */

test('Updates', async () => {
	jest.setTimeout(100);
	expect.assertions(1);

	try {
		let response = null;

		//Update Test
		profiler.set('Locking');
		for(let i = 0 ; i < ids.length; i++){
			dao.users.queueSelectForUpdate(ids[i]);
			response = await execStatement(dao.users.load(1, 1000));
			
			if ( response.results.table[0].data.length === 0 ) throw new Error('UPDATE-LOCK: No record found with id: ' + ids[i]);
		}

		profiler.set('Queuing Updates');
		for(let i = 0 ; i < ids.length; i++){
			let args = {
				NAME: 'user-'+ids[i],
				LAST_LOGIN: new Date(), 
				USER_SETTINGS: '{ status: 1 }', 
				ID: ids[i]
			};

			dao.users.update(args);
		}

		profiler.set('Executing');
		response = await execStatement(coreQueue.execute(1));
		profiler.set('Done');
		profiler.show();

		expect(response.code).toBeNull();

	} catch (error){
		logger.error(error);
	}
});

test('Deletes', async () => {
	jest.setTimeout(100);
	expect.assertions(1);

	try {
		let response = null;

		//Delete Test
		profiler.set('Locking');
		for(let i = 0 ; i < ids.length; i++){
			dao.users.queueSelectForUpdate(ids[i]);
			response = await execStatement(dao.users.load(2, 1000));
			
			if ( response.results.table[0].data.length === 0 ) throw new Error('DELETE-LOCK: No record found with id: ' + ids[i]);
		}

		profiler.set('Queuing Deletes');
		for(let i = 0 ; i < ids.length; i++){
			let args = {
				ID: ids[i]
			};

			dao.users.delete(args);
		}

		profiler.set('Executing');
		response = await execStatement(coreQueue.execute(2));
		profiler.set('Done');
		profiler.show();

		expect(response.code).toBeNull();

	} catch (error){
		logger.error(error);
	}
});

test('Inserts', async () => {
	jest.setTimeout(100);
	expect.assertions(1);

	try {
		let response = null;

		//Insert Test
		profiler.set('Queuing Inserts');
		for(let i = 0 ; i < ids.length; i++){
			let args = {
				ID: ids[i],
				NAME: 'user-'+ids[i],
				LAST_LOGIN: new Date(),
				USER_SETTINGS: '{ status: 1 }'
			};

			dao.users.insert(args);
		}

		profiler.set('Executing');
		response = await execStatement(coreQueue.execute(2));
		profiler.set('Done');
		profiler.show();

		expect(response.code).toBeNull();

	} catch (error){
		logger.error(error);
	}
});

test('MixTests', async () => {
	jest.setTimeout(100);
	expect.assertions(1);

	try {
		let response = null;

		//Mix Test
		profiler.set('Queuing Operations');
		for( let i = 0 ; i < MIX_VB_STATEMENT.length; i++ ){
			let vbStmt = MIX_VB_STATEMENT[i];

			let [ table, op ] = vbStmt.statement.split('_');

			let curDao = dao[table.toLocaleLowerCase()];
			let method = op.toLowerCase();

			if ( op !== 'INSERT' ){
				curDao.queueSelectForUpdate(vbStmt.args.ID);
				response = await execStatement(curDao.load(3, 1000));
				
				if ( response.results.table[0].data.length === 0 ) throw new Error('MIX-LOCK: No record found with id: ' + ids[i]);	
			}

			//dao.(users|items).(insert|delete|update)( entity )
			curDao[method]( vbStmt.args );
		}

		profiler.set('Executing');
		response = await execStatement(coreQueue.execute(3));
		profiler.set('Done');
		profiler.show();

		expect(response.code).toBeNull();

	} catch (error){
		logger.error(error);
	}
});

/*
		//Mix Test
		
});
//*/