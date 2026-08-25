# QUAL-12: refusing to weaken a security posture

The one thing a security configuration must never do is silently give
you less than you asked for. clink states what it will and will not
accept as a **declared matrix of refusals** - fifteen rows covering the
engine's own control plane, its Kafka client configuration, and its
Postgres transport - and every row is measured, five of them against
real servers rather than mocks. **Fifteen of fifteen behave as
declared.**

| Provenance | |
|---|---|
| Matrix | `qualification/qual12/refusals.json` - each row declares the misconfiguration, the outcome clink must produce, and the reason it is declared that way, before it is measured |
| Run | `qual12-local-c`, engine `0ca5936`: 15/15 rows as declared, including all 5 that need a real server |
| Rig | none - no cloud rig and no cost. Every refusal here manifests while parsing arguments, before a socket is opened, so it needs the binary and its image rather than a cluster ([why](README.md#the-rig)) |
| Live rows | Kafka SASL refusals against a real broker; Postgres transport against real servers with TLS off and on |
| Shipped-binary check | the control-plane refusals run against a built image, before and after the fix - a validator the binary never calls would refuse nothing, and a unit test cannot see that wiring |

## The three outcomes, and why WARN exists

A refusal matrix with only refusals would be satisfied by an engine that
rejected every configuration - secure the way a brick is secure. Each
surface therefore declares accept rows too, and the vocabulary has three
outcomes:

- **REFUSE** - the operator asked for a protection this deployment
  cannot provide, so nothing starts or connects.
- **WARN** - the operator asked for nothing specific and the environment
  supplied something weak. Plaintext to a database on a private network
  is a legitimate deployment and refusing it would break real users to
  no benefit; the requirement is that the fact is never *silent*.
- **ACCEPT** - a legitimate configuration that must keep working,
  including deliberate plaintext. An operator who asked for nothing and
  got nothing has not been downgraded.

A row that could not be exercised is reported as **unexercised**, never
as a pass. That distinction is the campaign's spine, and it is not
hypothetical: this campaign's own premise was wrong before it began. The
programme's audit recorded that the live test suite "already proves
refusal shapes against real SASL/TLS brokers and Postgres sslmode". It
does not - the live runner has no security leg at all. An unproven row
and a missing row must never look alike.

## What the campaign found

**The control plane did not follow its own doctrine.** clink refuses
credentials without a protecting protocol in its Kafka connector, and
its Postgres connector asserts the *established* transport with the rule
written out: "asked for TLS and did not get it -> throw". Its own
control plane followed neither. Four configurations came up **plaintext
on a port the operator had configured for TLS**:

1. A TLS flag on a binary built without TLS support - logged a warning
   and served plaintext. A log line is not a security control; the
   process still answered.
2. `--tls-cert` without `--tls-key` - the enabling condition was
   `cert && key` with no else branch, so the listener came up plaintext
   with **nothing logged at all**.
3. `--tls-client-ca` alone - mTLS requested, no server certificate, the
   same silent plaintext listener, verifying nothing.
4. A worker's client certificate and key without `--tls-ca` - client
   credentials configured for a connection that is not protected at all.

Three of the four are single-flag typos of a correct configuration, and
two are entirely silent. Both roles now validate the argument set before
binding or connecting and refuse to start, each message naming what the
refusal prevented (followups item 81).

**The Postgres transport guard had never been exercised.** It was wired
into all three Postgres sinks and nothing proved it. It is now measured
against real servers: an established, unencrypted connection whose
conninfo demanded TLS is refused by clink itself; a connection with no
`sslmode` that comes up plaintext is accepted but stated, naming both
options that resolve it; an encrypted connection is recorded so an
operator can prove from the log what the transport actually was.

## What this qualifies

- **Demonstrated:** fifteen declared security refusals measured as
  declared at engine `0ca5936`, including live proof against a real
  SASL broker and real Postgres servers with TLS both off and on, and
  three of the four control-plane refusals proven in a built image
  rather than only as unit cases - measured against a pre-fix image as
  well as a fixed one.
- **Tested but bounded:** the matrix covers the control plane, the Kafka
  client configuration and the Postgres transport. It is a matrix, not a
  survey - a surface with no row is a surface with no claim.
- **Architecturally supported but not qualified:** certificate rotation
  and expiry behaviour, revocation, TLS for the other connectors
  (RabbitMQ, S3, ClickHouse, MySQL), and authorisation as distinct from
  transport security.
- **Unknown:** anything an attacker does rather than an operator
  misconfigures. This campaign is about not silently weakening what was
  asked for; it is not a penetration test and makes no claim to be.

## Caveats

- The shipped-binary check proves three of the four control-plane
  refusals and the accept row. The fourth - "TLS requested on a build
  without TLS support" - cannot be exercised by any image clink ships,
  because they all link TLS; the smoke reports it unexercised there
  rather than passing it quietly, and it is proven as a unit case
  instead. That is why the validator takes the linkage as a parameter
  rather than reading the compile-time flag internally: it makes the
  unbuildable case expressible.
- Two tests in this campaign had to be corrected after they passed for
  the wrong reason, and both corrections are worth stating: an
  `sslmode=require` row initially passed with clink's own guard disabled
  because *libpq* refuses first - it was measuring a dependency, not the
  engine - and the runner first reported two rows as misbehaving when
  their broker fixture had simply failed to start. A check that cannot
  fail, and a failure blamed on the wrong component, are both ways a
  green matrix can mean nothing.

## The before and after, in a shipped artifact

The clearest evidence this campaign produced is not a green run. The
same smoke, pointed at the **published pre-fix image**, records ACCEPT
on all three control-plane refusal rows: a coordinator starting happily
with an incomplete TLS pair, a coordinator starting with mTLS requested
and no certificate, a worker connecting with client credentials and no
protected transport. That image was published and could have been
deployed. Pointed at the published image built from the fix
(`sha-0ca5936d72f0-faultinj`, amd64), the same three rows record
REFUSE, each naming what it prevented.

Raw evidence (the matrix as run, per-row results, and both image-smoke
readings) is retained; every number above is taken from it.
