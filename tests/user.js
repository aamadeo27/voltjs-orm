const core = require('../core');

let schema = [ 
	{name: 'ID', type: 'long'},
	{name: 'NAME', type: 'string'},
	{name: 'LAST_LOGIN', type: 'date'},
	{name: 'USER_SETTINGS', type: 'string'}
];

core.publishModel('USERS', schema, 'ID', 'ID');

core.publishStatement('USERS_LOAD_BY_NAME','select * from users where name = ?', 'USERS', ['string']);