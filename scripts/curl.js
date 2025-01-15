const fs = require("fs/promises");
const crypto = require("crypto");

function createReq(reqType, userId, once = true, cookieStr = undefined) {
  if (reqType === "chzzk_live") {
    const req = { uid: userId, once: once };
    if (cookieStr) req["cookies"] = cookieStr;
    return { reqType: reqType, chzzkLive: req };
  } else if (reqType === "afreeca_live") {
    return { reqType: reqType, afreecaLive: { userId: userId, once: once } };
  } else {
    throw new Error(`Unknown request type: ${reqType}`);
  }
}

function decrypt(encryptedText, key) {
  if (key.length !== 32) throw new Error("Key must be 32 bytes");
  const inputBuffer = Buffer.from(encryptedText, "base64");
  const decipher = crypto.createDecipheriv("aes-256-cfb", Buffer.from(key), inputBuffer.subarray(0, 16));
  const decrypted = Buffer.concat([decipher.update(inputBuffer.subarray(16)), decipher.final()]);
  return decrypted.toString("utf8");
}

(async () => {
  const [url, reqType, userId, hasCookie] = process.argv.slice(2);

  // get cookie string
  let cookieStr = undefined;
  if (hasCookie === "true") {
    const authed = JSON.parse(await fs.readFile("../dev/curl.json", 'utf-8'))["authed"];
    cookieStr = decrypt((await (await fetch(authed["url"])).json())["encrypted"], authed["enckey"]);
  }

  // send request
  const res = await fetch(url, {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify(createReq(reqType, userId, true, cookieStr)),
  });
  console.log(await res.text())
})();
