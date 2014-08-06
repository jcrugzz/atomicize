
var EE = require('events').EventEmitter;
var trigger = require('level-trigger');
var batch = require('level-create-batch');

module.exports = Atomicize;

//
// We are just wrapping level-trigger for now and making it more atomic as we
// don't want to queue jobs to be pending if they are already being executed,
// a new one will come if that doesn't fix things
//
function Atomicize(options, fn) {
  if (!(this instanceof Atomicize)) return new Atomicize(options, fn);
  options = options || {};

  // actual level database
  this.db = options instanceof levelup
    ? options
    : options.db;

  if (!this.db) throw new Error('Must pass in database');

  this.fn = fn;
  this.name = options.name || 'jobs'
  this.trigger = trigger(this.db, this.name, this.fn);

  this.trigger.on('complete', this.emit.bind(this, 'complete'));
  this.trigger.on('done', this._onDone.bind(this));
  // Object of job keys being processed
  this.processing = {};
}

// This is a weird API in all honesty as we have a terminating work function
// and this function only really serves to satisfy a possible early error
// condition
Atomicize.prototype.queue = function (data, fn) {
  // assumes data contains a key and a value
  if (!data.key || !data.value)
    return process.nextTick(function () {
      fn(new Error('Malformed data'));
    });
  // Should I make this an error case also?
  if (this.processing[data.key])
    return process.nextTick(function () {
      fn(null, 'In-progress');
    });

  this.processing[data.key] = true;
  batch(this.trigger, [data], fn);
};

Atomicize.prototype._onDone = function (data) {
  var error = new Error('wtf there is a problem');
  error.body = data;
  if(!this.processing[data.key])
    return this.emit('error', error);

  delete this.processing[data.key];
};
