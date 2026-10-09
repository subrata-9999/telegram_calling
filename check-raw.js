// Usage: node check-raw.js audio\incoming_XXXX.raw
// Prints size/loudness of a raw recording and saves a playable .wav copy.
// Assumes raw PCM, mono, 48000 Hz, 16-bit little-endian.
const fs = require("fs");

const file = process.argv[2];

if (!file) {
  console.log("Usage: node check-raw.js <file.raw>");
  process.exit(1);
}

const buf = fs.readFileSync(file);
const samples = Math.floor(buf.length / 2);

let peak = 0;
let nonZero = 0;
let sumSq = 0;

for (let i = 0; i < samples; i++) {
  const v = buf.readInt16LE(i * 2);
  const a = Math.abs(v);
  if (a > peak) peak = a;
  if (v !== 0) nonZero++;
  sumSq += v * v;
}

const rms = samples ? Math.sqrt(sumSq / samples) : 0;

console.log("File: " + file);
console.log("Size: " + buf.length + " bytes");
console.log("Length if mono 48 kHz: " + (samples / 48000).toFixed(1) + " seconds");
console.log("Loudest sample: " + peak + " of 32767");
console.log("Average level (RMS): " + Math.round(rms));
console.log("Non-silent samples: " + (samples ? ((100 * nonZero) / samples).toFixed(1) : "0") + "%");

if (buf.length === 0) {
  console.log("RESULT: the file is EMPTY. Nothing was recorded from B.");
} else if (peak < 100) {
  console.log("RESULT: the file has data but it is SILENT.");
} else {
  console.log("RESULT: the file contains SOUND.");
}

// Save a .wav copy so any player can open it.
const header = Buffer.alloc(44);
header.write("RIFF", 0);
header.writeUInt32LE(36 + buf.length, 4);
header.write("WAVE", 8);
header.write("fmt ", 12);
header.writeUInt32LE(16, 16);
header.writeUInt16LE(1, 20);      // PCM
header.writeUInt16LE(1, 22);      // mono
header.writeUInt32LE(48000, 24);  // sample rate
header.writeUInt32LE(96000, 28);  // byte rate
header.writeUInt16LE(2, 32);      // block align
header.writeUInt16LE(16, 34);     // bits per sample
header.write("data", 36);
header.writeUInt32LE(buf.length, 40);

const wavPath = file.replace(/\.raw$/i, "") + ".wav";
fs.writeFileSync(wavPath, Buffer.concat([header, buf]));
console.log("Playable copy saved: " + wavPath);