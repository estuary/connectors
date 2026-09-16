#!/usr/bin/env python3
"""Generate the audio files the Seeding requests upload.

Gong drops uploaded calls it decides are too short, measuring duration *after*
trimming blank audio from each end. An earlier version of this script produced
sine tones, which Gong accepted at upload (201) and then silently discarded —
most likely because voice-activity detection treats a pure tone as blank, so
the trimmed length is zero no matter how long the file is. These files
therefore carry real synthesized speech, comfortably past the 60 second floor.

Requires macOS: uses `say` for synthesis. Regenerate after a fresh clone with:

    python3 bruno/seed-media/generate.py
"""

import pathlib
import subprocess
import wave

HERE = pathlib.Path(__file__).parent

# Gong drops calls under 60s (post-trim); these targets leave real headroom.
TARGET_SECONDS = {"seed-call-a.wav": 90, "seed-call-b.wav": 150}

# Distinct voice and script per file — Gong requires each call's media to be
# unique audio, so the two must not be interchangeable.
SCRIPTS = {
    "seed-call-a.wav": (
        "Samantha",
        "Thanks for taking the time today. I wanted to walk through how the "
        "data pipeline is set up on your side, and where the current "
        "bottlenecks are. On our end we capture changes as they happen and "
        "land them in your warehouse within a few seconds. ",
    ),
    "seed-call-b.wav": (
        "Daniel",
        "Following up on our last conversation about the migration timeline. "
        "The team has finished the schema review, and we are ready to start "
        "backfilling historical records next week. I would like to confirm "
        "the cutover window before we commit to a date. ",
    ),
}


def synthesize(path: pathlib.Path, voice: str, text: str) -> float:
    subprocess.run(
        ["say", "-o", str(path), "--file-format=WAVE",
         "--data-format=LEI16@16000", "-v", voice, text],
        check=True,
    )
    with wave.open(str(path)) as w:
        return w.getnframes() / w.getframerate()


if __name__ == "__main__":
    for name, target in TARGET_SECONDS.items():
        voice, paragraph = SCRIPTS[name]
        path = HERE / name
        # Repeat the script until the rendered audio clears the target, rather
        # than guessing a word count — synthesis rate varies by voice.
        repeats = 1
        while True:
            seconds = synthesize(path, voice, paragraph * repeats)
            if seconds >= target:
                break
            repeats += 1
        print(f"wrote {path} ({seconds:.1f}s, {path.stat().st_size} bytes, voice {voice})")
