const VoltConfiguration = require('voltjs/lib/configuration');

const config = new VoltConfiguration();
config.host = 'volthost';
config.port = 21212;
config.username = 'operator';
config.password = 'mech';
config.flushInterval = 1;