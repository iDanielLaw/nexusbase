#!/usr/bin/env node
'use strict';

var path = require('path');
var execFile = require('child_process').execFile;

var exe_ext = process.platform === 'win32' ? '.exe' : '';

var protoc = 'protoc' + exe_ext;

var cmd_ext = process.platform === 'win32' ? '.cmd' : '';


var plugin = path.resolve('./', 'node_modules/.bin/protoc-gen-ts' + cmd_ext);

var args = ['--plugin=protoc-gen-ts=' + plugin].concat(process.argv.slice(2));

var child_process = execFile(protoc, args, function(error, stdout, stderr) {
  if (error) {
    console.error('Error: Failed to execute protoc. See details below.');
    // The stderr from protoc usually contains the most helpful error message.
    if (stderr) {
      console.error(stderr);
    }
    console.error(error); // The error object contains details like the exit code.
    process.exit(1);
  }
});

child_process.stdout.pipe(process.stdout);
child_process.stderr.pipe(process.stderr);
