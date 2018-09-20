const generator = require('./../gen/Generator');
const VoltClient = require('voltjs/lib/client');
const { LOGIN_ERRORS } = require('voltjs/lib/voltconstants');
const { execStatement } = require('../util');
const config = require('./config');

require('./user');
require('./item');

const client = new VoltClient([config]);

const sufixes = [ 'SPLoader', 'SPLLoader', 'MPLoader', 'MPLLoader', 'SPWriter', 'MPWriter' ];
const dropProcedure = proc => `DROP PROCEDURE ${proc} IF EXISTS; `;
const dropProcedures = prefix => sufixes.map( sufix => dropProcedure(prefix + sufix));

const DROP = table => 'DROP TABLE ' + table + ' IF EXISTS;';
const CREATE_USERS = `CREATE TABLE USERS ( 
    ID INT NOT NULL, 
    NAME VARCHAR(32) NOT NULL,
    LAST_LOGIN TIMESTAMP NOT NULL,
		USER_SETTINGS VARCHAR(1024),
		PRIMARY KEY(ID)
);`;
const CREATE_ITEMS = `CREATE TABLE ITEMS ( 
		ID VARCHAR(32) NOT NULL, 
		GROUP_ID VARCHAR(32) NOT NULL, 
		NAME VARCHAR(32) NOT NULL,
		DESCRIPTION VARCHAR(32) NOT NULL,
    VALID_SINCE TIMESTAMP NOT NULL,
		VALID_UNTIL TIMESTAMP NOT NULL,
		PRIMARY KEY(ID)
);`;
const PARTITION = table => 'PARTITION TABLE ' + table + ' ON COLUMN ID;';

const PRE_SQL = [
	dropProcedure('SavecoreHashId') + dropProcedure('GetcoreHashId'),
	dropProcedures('Example').join('\n'),
	DROP('USERS') + DROP('ITEMS'),
	DROP('core_PROCEDURES') + DROP('LOCKS'),
	CREATE_USERS + CREATE_ITEMS,
	PARTITION('USERS') +	PARTITION('ITEMS')
];

beforeAll( async () => {
	const { connected, errors } = await client.connect();

	if ( !connected ) {
		throw new Error( errors.reduce( (desc, err) => desc + `${err}:${LOGIN_ERRORS[err]}`, '') );
	}

	for( let i in PRE_SQL ){
		let sql = PRE_SQL[i];
		await execStatement(client.adHoc(sql), sql);
	}
	
}, 15000);

afterAll( () => {
	return client.exit();
});

test('Generator', async () => {
	jest.setTimeout(20000);
	return generator(client, 'Example', 'org.aa.test');
});