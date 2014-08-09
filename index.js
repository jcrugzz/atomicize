
var EE = require('events').EventEmitter;
var util = require('util');
var trigger = require('level-trigger');
var batch = require('level-create-batch');

module.exports = Atomicize;

util.inherits(Atomicize, EE);

//
// We are just wrapping level-trigger for now and making it more atomic as we
// don't want to queue jobs to be pending if they are already being executed,
// a new one will come if that doesn't fix things
//
function Atomicize(options, fn) {
  if (!(this instanceof Atomicize)) return new Atomicize(options, fn);
  options = options || {};
  EE.call(this);

  // actual level database (TODO: have better check here);
  this.db = typeof options.get == 'function'
    ? options
    : options.db;

  if (!this.db) throw new Error('Must pass in a levelup like database');

  this.fn = fn;
  this.name = options.name || 'jobs'
  this.trigger = trigger(this.db, this.name, this.fn);

  this.trigger.on('complete', this.emit.bind(this, 'complete'));
  this.trigger.on('done', this.invalidate.bind(this));
  // Object of job keys being processed
  this.processing = {};
}

// This is a weird API in all honesty as we have a terminating work function
// and this function only really serves to satisfy a possible early error
// condition
Atomicize.prototype.queue = function (data, fn) {
  // assumes data contains a key and a value
  if (!data || !data.key || !data.value)
    return process.nextTick(function () {
      fn(new Error('Malformed data'));
    });
  // Should I make this an error case also?
  if (this.processing[data.key])
    return process.nextTick(function () {
      fn(null, 'In-progress');
    });

  this.processing[data.key] = true;
  batch(this.trigger, [data], this._onBatch.bind(this, fn, data));
};

Atomicize.prototype._onBatch = function (fn, data, err) {
  if (err) {
    this.invalidate(data);
    return fn(err);
  }
  this.emit('queued', data);
};

// Invalidate that a current key is being processed when finished or errors
Atomicize.prototype.invalidate = function (data) {
  var key = typeof data === 'string' ? data : data && data.key;
  if (key && this.processing[key]) delete this.processing[key];
  this.emit('done', data);
  return this;
};
