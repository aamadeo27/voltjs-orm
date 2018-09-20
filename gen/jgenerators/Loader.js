const path = require('path');
const fs = require('fs');
const { mkdir  } = require('../../util');

const header =  (packageName, procName, locking) => `
package ${packageName};

import org.aa.Bulk${locking?'Locking':''}Loader;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

public class ${procName} extends Bulk${locking?'Locking':''}Loader {
`;

const runMethod = (sp, locking) => `
\tpublic VoltTable[] run(${sp ? 'String partitionKey, ' : ''}VoltTable [] loadOperations${locking ? ', long transactionId, long lifespan' : ''} ){
\t\tthis.debug = new VoltTable(DEBUG_SCHEMA);	
\t\treturn super.bulkLoad(${locking ? 'transactionId' : '-1'}, loadOperations, ${locking ? 'lifespan' : '0'}, ${sp ? 'partitionKey' : 'getSeededRandomNumberGenerator().nextLong() + ""'});
\t}
}
`;

const blue = str => '\x1b[34m' + str + '\x1b[37m';

const SPLoader = async (base, packageName, namespace, statements, sp, locking) => {
	let srcdir = `${base}/${namespace}/src/${packageName.replace(/\./g,'/')}`;
	srcdir = srcdir.replace(/\\/g,'/').replace(/\//g, path.sep);

	await mkdir(srcdir);
	srcdir += path.sep;
	
	const procName = `${namespace}${sp ? 'SP' : 'MP' }${locking ? 'L' : ''}Loader`;
	let filepath = `${srcdir}${procName}.java`;
	process.stdout.write( blue('[Generating] ') + filepath + '\n');
	
	const filestream = fs.createWriteStream(filepath);

	filestream.write(header(packageName, procName, locking));
	statements.forEach( statement => filestream.write(statement) );
	filestream.write( runMethod(sp,locking) );

	await new Promise( resolve => filestream.end( resolve ));
};

module.exports = SPLoader;