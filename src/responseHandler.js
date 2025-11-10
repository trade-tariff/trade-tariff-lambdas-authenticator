async function handler(event) {
  const { request, response } = event.Records[0].cf;

  response.headers["x-ratelimit-limit"] =
    request.headers["x-ratelimit-limit"] || [];
  response.headers["x-ratelimit-remaining"] =
    request.headers["x-ratelimit-remaining"] || [];
  response.headers["x-ratelimit-reset"] =
    request.headers["x-ratelimit-reset"] || [];

  return response;
}

module.exports.handler = handler;
