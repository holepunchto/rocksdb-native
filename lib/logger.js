/**
 * @typedef {object} Logger
 * @property {(...data: any[]) => void} log
 */

/** @type {Logger} */
const noLogger = {
  log: noop
}

exports.noLogger = noLogger

function noop() {}
