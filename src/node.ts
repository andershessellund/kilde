// ---------------------------------------------------------------------------
// kilde/node — bridges to Node.js streams
//
//   import { fromReadable, toReadable, intoWritable } from 'kilde/node';
// ---------------------------------------------------------------------------

export { fromReadable } from './node-streams/from-readable.js';
export { toReadable, sourceToReadable } from './node-streams/to-readable.js';
export { intoWritable, sourceIntoWritable } from './node-streams/into-writable.js';
export { PrematureCloseError } from './node-streams/premature-close.js';
