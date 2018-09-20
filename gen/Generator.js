const core = require('../core');
const digest = require('murmurhash-native').murmurHash128x64;
const VoltProcedure = require('voltjs/lib/query');
const GetcoreHashId = new VoltProcedure('GetcoreHashId',['string']);
const SavecoreHashId = new VoltProcedure('SavecoreHashId',['string','string']);
const { mkdir, execStatement, logger } = require('../util');
const path = require('path');
const fs = require('fs');
const { exec } = require('child_process');

const blue = str => '\x1b[34m' + str + '\x1b[37m';
const red = str =>  '\x1b[31m' + str + '\x1b[37m';

const ddl = {
	locks: {
		create: ` create table locks(
			partition_key	varchar(64)	not null,
			rowid bigint not null,
			transaction_id bigint not null,
			validity timestamp not null,
			primary key (partition_key, rowid)
		)`,
		partition: 'partition table locks on column partition_key'
	},
	core_procedures: {
		create: `create table core_procedures (
			namespace varchar(32) not null,
			hashid varchar(32) not null,
			primary key (namespace)
		)`,
		partition: 'partition table core_procedures on column namespace'
	},
	GetcoreHashId: `create procedure GetcoreHashId 
		partition on table core_procedures column namespace as
		select * from core_procedures where namespace = ?`,
	SavecoreHashId: `create procedure SavecoreHashId 
		partition on table core_procedures column namespace as
		upsert into core_procedures values (?,?)
	`,
	coreProcedure: (partition, fqcn) => `create procedure ${partition} from class ${fqcn};\n`
};

const JavaSourceGenerators = { 
	Loader: require('./jgenerators/Loader'),
	Writer: require('./jgenerators/Writer')
};

const toJSQLStmt = statement => {
	if ( !statement.sql ) return;

	return `\tpublic final SQLStmt ${statement.name} = new SQLStmt("${statement.sql}");\n`;
};

const ALL_PROCEDURES = [
	'SPWriter',
	'SPLoader',
	'SPLLoader',
	'MPWriter',
	'MPLoader',
	'MPLLoader'
];

const addStatement = (statements, proc, jsql) => {
	if ( !statements[proc] ) return;

	statements[proc].push(jsql);
};

const getHashId = (client, namespace) => {
	const stmt = GetcoreHashId.getQuery();
	stmt.setParameters([namespace]);

	return client.callProcedure(stmt);
};

const updateHashId = (client, namespace, hash) => {
	const stmt = SavecoreHashId.getQuery();
	stmt.setParameters([namespace, hash]);

	return client.callProcedure(stmt);
}

const getVoltdbJar = (version = '8.3') => {
	if ( !process.env.VOLTDB_HOME ) throw new Error('Environment variable VOLTDB_HOME is not set');

	const jarpath = `${process.env.VOLTDB_HOME}/voltdb/voltdb-${version}.jar`;
	
	if ( !fs.statSync(jarpath).isFile() ) throw new Error(`voltdb.jar not found in ${jarpath}`);

	return jarpath.replace(/\//,path.sep);
};

const prepareTables = (client, tables) => {
	let ddlFull = '';

	if ( !tables.find( t => t === 'LOCKS') ){
		ddlFull += '\n' + ddl.locks.create + ';';
		ddlFull += '\n' + ddl.locks.partition + ';';
	}

	if ( !tables.find( t => t === 'core_PROCEDURES') ){
		ddlFull += '\n' + ddl.core_procedures.create + ';';
		ddlFull += '\n' + ddl.core_procedures.partition + ';';
	}

	return ddlFull;
};

const prepareHashProcedures = (client, procedures) => {
	let ddlFull = '';

	if ( !procedures.find( t => t === 'GetcoreHashId') ){
		ddlFull += '\n' + ddl.GetcoreHashId + ';';
	}

	if ( !procedures.find( t => t === 'SavecoreHashId') ){
		ddlFull += '\n' + ddl.SavecoreHashId + ';';
	}

	return ddlFull;
};

const generate = async (client, namespace, packageName, procedures = ALL_PROCEDURES) => {

	if ( !process.env.VOLTDB_HOME ){
		throw new Error('Environment Variable VOLTDB_HOME is not set');
	}

	let response = await execStatement(client.systemCatalog('tables'));
	let ddlInit = prepareTables(client, response.results.table[0].data.map(t => t.TABLE_NAME ));
	if ( ddlInit ) await execStatement(client.adHoc(ddlInit), ddlInit);

	response = await execStatement(client.systemCatalog('procedures'));
	let definedProcedures = response.results.table[0].data.map( p => p.PROCEDURE_NAME );
	ddlInit = prepareHashProcedures(client, definedProcedures);
	if ( ddlInit ) await execStatement(client.adHoc(ddlInit), ddlInit);

	const hashCall = getHashId(client, namespace);
	const statements = {};
	procedures.forEach( proc => statements[proc] = []);

	const selectRegexp = /(with \w+ as \(.+\))?\s*select.+/i;
	for( let entry of core.getStatements() ){
		let statement = entry[1];

		if ( statement.sql === core.constants.NOT_GENERATE ) continue;

		const read = !!statement.sql.match(selectRegexp);
		const mp = !!statement.model && !statement.model.partitionKey;
		const hasPk = !!statement.model && !!statement.model.pk;
		
		let jsql = toJSQLStmt(statement);

		if ( read ){
			addStatement(statements,'MPLoader',jsql);
			addStatement(statements,'SPLoader',jsql);

			if ( hasPk ) {
				addStatement(statements,'MPLLoader',jsql);
				if ( !mp ) addStatement(statements,'SPLLoader',jsql);
			}
		} else {
			if (!mp) addStatement(statements,'SPWriter',jsql);
			addStatement(statements,'MPWriter',jsql);
		}
	}

	const bytes = Buffer.from(JSON.stringify(statements));
	const hash = digest(bytes, 0, bytes.length, 0);

	response = await execStatement(hashCall);
	const table = response.results.table[0].data;
	const lastHash = table[0] && table[0].HASHID || '';

	if ( hash === lastHash ){
		process.stdout.write('\n[Stoping] Procedures are updated \n');
		return Promise.resolve();
	}

	process.stdout.write('Update Procedures of ' + namespace + '\n\n');
	const base = path.join(__dirname,'generated');
	const libdir = path.join(__dirname,'lib');
	const corejar = path.join(libdir, 'volt-orm.jar');
	const packageSpec = packageName.split('.');
	const jarSep = ';';
	const voltdbjar = getVoltdbJar();
	const classpath = [voltdbjar, corejar].join(jarSep);
	const sourcepath = path.join(base, namespace, 'src');
	const targetdir = path.join(base, namespace, 'target');
	const jarpath = path.join(targetdir, namespace + '.jar');
	const classdir = path.join(targetdir,'classes');
	const pkgdir = path.join(sourcepath, packageSpec.join(path.sep),'*.java');

	mkdir(classdir);
	mkdir(libdir);

	//Load coreity Procedures
	if ( !lastHash ){
		await execStatement(client.updateClasses(corejar), 'Load coreity.jar');
	}

	for( let i = 0 ; i < procedures.length ; i++){
		let p = procedures[i];
		let sp = p.match('SP');
		let locking = p.match('LLoader');
		let loader = p.match('Loader');

		if ( loader ){
			await JavaSourceGenerators.Loader(base, packageName, namespace, statements[p], sp, locking);
		} else {
			await JavaSourceGenerators.Writer(base, packageName, namespace, statements[p], sp);
		}
	}

	let compile = `javac -verbose -classpath ${classpath} -d ${classdir} -sourcepath ${sourcepath} ${pkgdir}`; 
	let archive = `jar cvf ${jarpath} -C ${classdir} .`;

	const execCmd = cmd => new Promise( (resolve, reject) => {
		process.stdout.write(blue('[Executing] ') + cmd + '\n');

		exec(cmd, (error, stdout, stderr) => {
			if ( error ) reject(stderr);
			resolve(stdout);
		});
	}).catch( err => 
		process.stderr.write(red(err) + '\n') 
	); 

	await execCmd(compile);
	await execCmd(archive);

	await execStatement(client.updateClasses(jarpath));

	definedProcedures = definedProcedures.filter( p => p.match(namespace + '.+'));

	let sql = '';
	for(let i = 0 ; i < procedures.length ; i++){
		let p = procedures[i];
		let partition = p.match('SP') ? 'partition on table locks column partition_key' : '';

		if ( ! definedProcedures.find( e => e === `${namespace}${p}`) ){
			sql += ddl.coreProcedure(partition, `${packageName}.${namespace}${p}`);
		}
	}

	await execStatement(client.adHoc(sql), sql + '\n');
	await execStatement(updateHashId(client, namespace, hash), 'Update Hash \n');

	return Promise.resolve();
};

module.exports = generate;