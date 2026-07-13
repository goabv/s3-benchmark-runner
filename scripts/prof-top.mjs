#!/usr/bin/env node
// Print the top self-time functions from a V8 .cpuprofile (e.g. a worker profile
// written by the benchmark's `profile` option). Diff two node versions' profiles
// to see which function regressed.
//   node scripts/prof-top.mjs <file.cpuprofile> [topN]
import { readFileSync } from 'node:fs';
import { printTop } from '../diagnostics/nodediff/proftop.mjs';

const file = process.argv[2];
if (!file) { console.error('usage: node scripts/prof-top.mjs <file.cpuprofile> [topN]'); process.exit(1); }
const topN = Number(process.argv[3] || 25);
printTop(JSON.parse(readFileSync(file, 'utf8')), topN, file);
