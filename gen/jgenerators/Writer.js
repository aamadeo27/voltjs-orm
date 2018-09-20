const path = require('path');
const { mkdir  } = require('../../util');
const fs = require('fs');

const blue = str => '\x1b[34m' + str + '\x1b[37m';

const header =  (packageName, procName) => `
package ${packageName};

import org.aa.coreWriter;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

public class ${procName} extends AtomicWriter {
`;

const runMethod = sp => `
\tpublic VoltTable run(${sp ? 'String partitionKey, ' : ''}long transactionId, VoltTable [] operations){
${ sp ? '\t\tthis.partitionKey = partitionKey;' : ''}
\t\tsuper.write(transactionId, operations);
\t\treturn result;
\t}
}
`;

const Writer = async (base, packageName, namespace, statements, sp) => {
	let srcdir = `${base}/${namespace}/src/${packageName.replace(/\./g,'/')}/`;
	srcdir = srcdir.replace(/\\/g,'/').replace(/\//g, path.sep);

	await mkdir(srcdir);

	const procName = `${namespace}${sp ? 'SP' : 'MP' }Writer`;
	let filepath = `${srcdir}${procName}.java`;
	const filestream = fs.createWriteStream(filepath);
	
	process.stdout.write(blue('[Generating] ') + filepath + '\n');

	filestream.write(header(packageName, procName));
	statements.forEach( statement => filestream.write(statement) );
	filestream.write( runMethod(sp) );

	await new Promise( resolve => filestream.end( resolve ));
};

module.exports = Writer;