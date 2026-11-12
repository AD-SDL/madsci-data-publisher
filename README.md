# MADSci Data Publisher

A read-only client that runs next to a [MADSci](https://github.com/AD-SDL/MADSci) lab and publishes what the lab is doing, so agents and remote users can follow it.

It reads the MADSci managers' HTTP APIs and never connects to a database. It collects two kinds of data:

- **Live state**, polled about every 30 seconds: node status and what each node is doing, active workflows and their current steps, experiment status, and selected events such as errors and step failures.
- **Site descriptions**, collected on request: the nodes in a workcell, their actions and arguments, locations, transfer paths and resource templates.

The client pushes updates out of the lab to a data store. Agents (for example through an MCP server) and dashboards read from that store, so nothing outside the lab needs to reach into it.

## Status

Early development, started at the RPL Agentic Hackathon at Argonne in November 2026. Interfaces will change.

## License

MIT. See [LICENSE](LICENSE).
