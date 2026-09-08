// API-MUTATE-ONLY-WITH-CONSENT inside the Bruno CLI. The SessionStart hook loads this file
// into every Node process in Claude's shell (NODE_OPTIONS=--require); it does nothing unless
// the process is `bru`.
//
// Every request file Bruno runs, `bru.runRequest` chains included, goes through
// runSingleRequest before its pre-request script runs. A request that changes provider data
// runs only when the connector's permissions file says `seeding: assistant` and the file is
// under bruno/Seeding/. Requests a script sends itself (`bru.sendRequest`, the OAuth refresh)
// are not checked: they carry no request file, and the refresh is a POST that only reads.
const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

const READ_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);

/** Bruno's runner module when this process is `bru` (`…/bin/bru.js`), else null. */
function brunoRunner() {
  if (!/\/bin\/bru\.js$/.test(process.argv[1] || "")) return null;

  const runner = path.resolve(
    process.argv[1],
    "..",
    "..",
    "src",
    "runner",
    "run-single-request.js",
  );

  return fs.existsSync(runner) ? runner : null;
}

/** The connector's permissions file, null when there is none. An unreadable one throws. */
function parsePermissions(connectorDir) {
  let common;

  try {
    common = execFileSync(
      "git",
      [
        "-C",
        connectorDir,
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
      ],
      {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
      },
    ).trim();
  } catch {
    return null; // not in a git repo, so not a create-capture-connector run
  }
  const file = path.join(
    common,
    "claude-permissions",
    path.basename(connectorDir) + ".json",
  );

  return fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : null;
}

/** A GraphQL read is a POST too, so a GraphQL request writes only when it holds a mutation. */
function writes(request) {
  if (request?.body?.mode === "graphql") {
    const query = String(request.body.graphql?.query || "").replace(
      /#.*$/gm,
      "",
    );
    return /(^|\})\s*(mutation|subscription)\b/.test(query);
  }
  return !READ_METHODS.has(String(request?.method || "").toUpperCase());
}

function refusalReason(item, collectionPath) {
  if (!writes(item.request)) return null;

  const connectorDir = path.dirname(collectionPath);
  const rel = path.relative(collectionPath, item.pathname || "");
  const consent = parsePermissions(connectorDir);

  if (consent?.seeding !== "assistant") {
    const state = consent
      ? `the user's answer is \`seeding: ${consent.seeding}\``
      : "there is no permissions file";
    return (
      `API-MUTATE-ONLY-WITH-CONSENT: ${rel} changes provider data, and ${state}. Human-in-the-loop with ` +
      "`seeding: user`: hand the request to the user. Otherwise leave the finding PENDING (GATE-SEEDING)."
    );
  }

  if (rel.split(path.sep)[0] !== "Seeding") {
    return `API-MUTATE-ONLY-WITH-CONSENT: ${rel} changes provider data, so it runs only from bruno/Seeding/.`;
  }

  return null;
}

const runnerPath = brunoRunner();
if (runnerPath) {
  const runner = require(runnerPath);
  const run = runner.runSingleRequest;
  // Replaced before run.js loads and destructures it, so every call goes through here.
  runner.runSingleRequest = async function (item, collectionPath, ...rest) {
    const reason = refusalReason(item, collectionPath);
    if (reason) {
      // Bruno prints only "Something went wrong" for a thrown error, so print the reason first.
      process.stderr.write(`permission gate: ${reason}\n`);
      throw new Error(reason);
    }
    return run.call(this, item, collectionPath, ...rest);
  };
}
