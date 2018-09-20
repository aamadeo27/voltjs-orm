const core = require('../core');

let schema = [ 
	{name: 'ID', type: 'string'},
	{name: 'GROUP_ID', type: 'string'},
	{name: 'NAME', type: 'string'},
	{name: 'DESCRIPTION', type: 'string'},
	{name: 'VALID_SINCE', type: 'date'},
	{name: 'VALID_UNTIL', type: 'date'}
];

core.publishModel('ITEMS', schema, 'ID', 'ID');

core.publishStatement('ITEMS_LOAD_BY_NAME','select * from items where name = ?', 'ITEMS', ['string']);