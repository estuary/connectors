#!/usr/bin/env python3
"""Download the audio files the Seeding requests upload.

Gong rejects recordings it cannot find speech in. Synthesized sine tones were
tried first and failed — the Gong UI reported "Call was recorded but the
recording doesn't contain any audio or video", and the calls never became
queryable. These are real human speech: public-domain LibriVox readings, two
different tales so the two calls carry distinct media as Gong requires.

Files land next to this script and are gitignored (~28MB each). Fetch them
after a fresh clone with:

    python3 bruno/seed-media/fetch.py

`generate.py` is the offline fallback — it synthesizes speech with macOS `say`
and needs no network. Whether synthesized speech satisfies Gong is untested;
real recordings are the safer default, which is why this script exists.
"""

import pathlib
import urllib.request

HERE = pathlib.Path(__file__).parent

# LibriVox, "The Type-Writer Girl" (archive.org item
# type-writer_girl_0909_librivox).
# Licence: http://creativecommons.org/licenses/publicdomain/
#
# Gong enforces media uniqueness across the whole account, so re-seeding needs
# audio it has never ingested before. Repoint these at unused chapters (or a
# different item) rather than re-running with files already attached to a call
# — the upload fails with an unhelpful generic "Bad request" if you do not.
SOURCES = {
    "seed-call-a.mp3": "https://archive.org/download/type-writer_girl_0909_librivox/typewritergirl_01_allen.mp3",
    "seed-call-b.mp3": "https://archive.org/download/type-writer_girl_0909_librivox/typewritergirl_02_allen.mp3",
}

# Runtimes as published by archive.org, mirrored into the `duration` field of
# the Seeding request that uploads each file. Gong re-measures the media and
# overrides what the request declares, so these only need to be close.
DURATIONS = {"seed-call-a.mp3": 789.51, "seed-call-b.mp3": 891.91}


if __name__ == "__main__":
    for name, url in SOURCES.items():
        target = HERE / name
        urllib.request.urlretrieve(url, target)
        size_mb = target.stat().st_size / 1024 / 1024
        print(f"wrote {target} ({size_mb:.1f} MB, {DURATIONS[name]}s)")
