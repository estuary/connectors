# OAuth

Flow supports a special type of connector configuration for sources/destinations that support OAuth. The idea is to enable users to easily grant us the required permissions to act on their behalf, without needing to generate and copy/paste API keys. Let's first look at how Flow expects the flow (😉) to work for OAuth-enabled connectors.

## The OAuth application
Before a user can grant us permissions to act on their behalf, we have to create an entity that represents Flow in the system in question -- this is called the OAuth application. Different providers have different requirements for their OAuth applications, as well as frequently requiring varying levels of validation before an application is allowed out of testing mode. But in general, an application will have the following properties:

### Name, logo, website, tagline, etc.
  Since the OAuth application represents Flow, it must to be able to be presented with the right name and in the right clothes. 
### Contact info
  The provider generally needs a way to contact the owner of the application for various reasons. Sometimes this needs to be verified before the application can go live.
### Client ID
  The Client ID is a string that uniquely identifies the application in the provider's system. This ID is allowed to be published, and is used to indicate which application the user is authorizing when they start the authorize flow, among other things
### Client Secret
  The Client Secret is a string that allows the owner of the application to perform "trusted" actions. It should never be published or exposed, as it can be used to gain access to the resources that users have granted to us (just like we do when we need that access ourselves).
### Redirect URI
  As explained below, the OAuth flow we're using involves a redirect step to a URI we control after the user has granted us permission. Usually there are strict limits on what this URI can be (must be HTTPS, must not include `#` fragments, etc.). For testing, you'll want to make sure you include `http://localhost:{port}/`, where `port` is emitted by `flowctl raw oauth` (usually it's `16963`, though)
## Scopes
  Frequently you'll be asked to select which "scopes" the application should be allowed to ask for. Conceptually, a scope should represent either a single action that can be performed to a single resource, or some other kind of flag or permission that the application is asking the user for. 
  
  You should select only the scopes that represent the level of access to the resources we need, and no more -- each additional scope increases the level of trust we're asking of the user, as well as widening the blast radius of a security breach. 

## The authorize step
The main operative concept in our use of OAuth is that of delegation. Rather than users just _giving_ us their own personal access tokens or API keys, we are asking for their permission to act on their behalf: it's still _our_ access key/secret that is performing the action, we're just doing it under permissions that were delegated to us by other users. So, it makes sense that the first step is to ask for, and get that consent.

### Redirect to the provider
First, the web UI (or `flowctl raw oauth`) will request a redirect URI to send the user who is requesting to initiate the OAuth flow. This URI is different for each provider, but generally contains a few key properties:
* `response_type=code` is part of the OAuth spec indicating that we're initiating an [Authorization Code flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.1).
* `client_id` is our OAuth application's client ID. Note that we do _not_ include `client_secret` here, as that would be visible to the user
* `redirect_uri` is a URL that we control where the user will be redirected after finishing the authorization grant
* `scope` represents the scopes that we're requesting. Usually this matches the scopes selected when creating the application, but conceptually it can be any subset of those scopes. Usually this is a comma- or space-separated, URL-encoded list of scopes. 
* `state`, `code_challenge`, and `code_challenge_method` are there to prevent various attacks. Conceptually, we compute a unique identifier for this particular OAuth request (`state`), and a hidden secret that we associate with that key. We then send the hash of that hidden secret along with the request, and perform various validation steps on it later to ensure that nobody is man-in-the-middling us. 
> **NOTE**: Since this step requires that the user be redirected to the provider (so that they can use their existing logged-in session, or log in as usual), all parameters to the authorize URL will be included as query-string parameters.

Here's an example authorization URL template:
```
https://airtable.com/oauth2/v1/authorize?
    response_type=code&
    client_id={{#urlencode}}{{{client_id}}}{{/urlencode}}&
    redirect_uri={{#urlencode}}{{{redirect_uri}}}{{/urlencode}}&
    state={{#urlencode}}{{{state}}}{{/urlencode}}&
    code_challenge={{#urlencode}}{{{code_challenge}}}{{/urlencode}}&
    code_challenge_method={{#urlencode}}{{{code_challenge_method}}}{{/urlencode}}&
    scope=data.records:read%20data.recordComments:read%20schema.bases:read
```

The user will now be redirected to the provider's UI for approval.

### Redirect back to us
After the user jumps through whatever hoops the provider requires in order to provide their consent, they will be redirected back to the provided `redirect_uri` with a few important query-paremeters along for the ride.

* `code` is a single-use token that we'll use to exchange for real credentuials representing Flow's application's delegated permission to act on behalf of the user.
* `code_challenge`, `code_challenge_method`, `state` will be used to enact various security precautions.

Our web UI will send this valuable payload up to the backend, where the next step of the process will happen: exchanging the `code` for an access (and refresh) token.

## The token exchange step
Once the backend receives the information from the authorize redirect, it will attempt to exchange that for real access credentials to the system in question. In order to do that, it must construct the authorize request. The authorize request is a POST request to the authorize endpoint. It is specified along 3 dimensions:

### The URL
Unlike GET requests where the query parameters live in the URL, since this is a POST request generally the URL will remain fixed. That being said, the `accessTokenUrlTemplate` field does have all of the template filling capabilities as any of the other templates. For example, Shopify needs the Store ID to be included as part of the domain name:
```json
{
    "accessTokenUrlTemplate": "https://{{{config.store}}}.myshopify.com/admin/oauth/access_token"
}
```

### The Body
For providers that expect query parameters in the body, they can be specified here. For example, the Google family of connectors look like this:
```json
{
    "accessTokenBody": "{\"grant_type\": \"authorization_code\", \"client_id\": \"{{{ client_id }}}\", \"client_secret\": \"{{{ client_secret }}}\", \"redirect_uri\": \"{{{ redirect_uri }}}\", \"code\": \"{{{ code }}}\"}"
}
```

### The Headers
Sometimes providers require that certain headers are specified. For example, sometimes query parmeters are expected to be form-encoded, in which case you can specify headers like so:
```json
{
    "accessTokenHeaders": { "content-type": "application/x-www-form-urlencoded" },
}
```

## The response
Once we successfully make the above token exchange request, we'll get back some kind of access token. This is what the connector will use to authenticate requests to the target system. Sometimes, the shape of the access token response doesn't match what we want: enter the `accessTokenResponseMap` field. This allows us to remap the access token response into our preferred format. In this contrived example, the access token response looks like this:
```json
{
    "access": "ey...==",
    "refresh": "CRYPTOGRAPHIC_RANDOM_REFRESH",
    "expires_in": 3600
}
```
The OAuth config has this:
```json
{
    "accessTokenResponseMap": {
        "access_token": "/access",
        "refresh_token": "/refresh",
        "token_expiry_date": "{{#now_plus}}{{ expires_in }}{{/now_plus}}",
    },
}
```
And the mapped access token response will look like this (where `token_expiry_date` is 1h in the future):
```json
{
    "access_token": "ey..==",
    "refresh_token": "CRYPTOGRAPHIC_RANDOM_REFRESH",
    "token_expiry_date": "2023-12-14T15:05:00Z"
}
```

## Where does it all go?
Great, we finished the OAuth flow! We initiated a request for the user to delegate access to our OAuth application, the user consented, we got a temporary authorization code, exchanged it for (somewhat more) permanent access credentials, and transformed those credentials into the shape needed for our connector. At this point, all that's left is to provide these credentials to the connector. 

By convention we have picked the field `credentials` to receive injected OAuth values. You should configure your connector's endpoint config to receive the following config in `credentials`: 
```jsonc
{
    "credentials": {
        "client_id": "...",
        "client_secret": "...",
        // All fields from the mapped access token response are included here, e.g
        "access_token": "ey..==",
        "refresh_token": "CRYPTOGRAPHIC_RANDOM_REFRESH",
        "token_expiry_date": "2023-12-14T15:05:00Z"
    }
}
```

In addition to configuring the endpoint config, for imported connectors you will also probably have to modify the config-reading logic to read OAuth data out of the `credentials` field specifically. 
> **Note:** Also, don't forget to make `credentials` a required field!

## Debugging
You can test the OAuth flow using `flowctl raw oauth`. Make sure that you have a local stack set up and running, as the intent is to emulate the production behavior as closely as possible. 

> **Note**: Make sure your terminal is set up to run the connector in question. If your `flow.yaml` is configured to use local commands, you should be inside the proper poetry shell for example.
