import json
import os
import urllib.request

body = open("build.log", "r", errors="replace").read()
tail = body[-55000:]

payload = json.dumps({
    "title": "Build failed - run " + os.environ["GITHUB_RUN_ID"],
    "body": "```\n" + tail + "\n```",
}).encode()

req = urllib.request.Request(
    "https://api.github.com/repos/" + os.environ["GITHUB_REPOSITORY"] + "/issues",
    data=payload,
    headers={
        "Authorization": "token " + os.environ["GH_TOKEN"],
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
    },
    method="POST",
)

try:
    resp = urllib.request.urlopen(req)
    print(resp.read().decode())
except urllib.error.HTTPError as e:
    print("GitHub API error:", e.code, e.read().decode())
    raise
