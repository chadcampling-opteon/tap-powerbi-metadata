# tap-powerbi-metadata

`tap-powerbi-metadata` is a Singer tap for Power BI metadata, built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Installation

Install from GitHub:

```bash
uv tool install git+https://github.com/chadcampling-opteon/tap-powerbi-metadata.git
```

## Configuration

### Accepted config options

- `tenant_id`: The Azure AD tenant that owns the Power BI service principal.
- `client_id`: The application (client) ID of the service principal.
- `client_secret`: The client secret used by the client-credentials flow.
- `start_date`: Optional. The earliest record date to sync.

A sample config file is available at [`.secrets/config.json.template`](.secrets/config.json.template). Run `tap-powerbi-metadata --about` for the full list of settings and capabilities.

### Source authentication and authorization

Access to the Power BI REST API requires a service principal. Someone with admin access to Microsoft Entra ID must create that principal and grant it access to the Power BI tenant.

Microsoft documents the process under [Automation with service principals](https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal). Creating a Microsoft Entra tenant is documented in [Create a Microsoft Entra tenant](https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-organization-create-azure-active-directory-tenant).

## Usage

You can run `tap-powerbi-metadata` by itself or in a pipeline using [Meltano](https://docs.meltano.com).

### Executing the tap directly

```bash
tap-powerbi-metadata --version
tap-powerbi-metadata --help
tap-powerbi-metadata --config CONFIG --discover > ./catalog.json
```

## Developer resources

Follow these instructions to contribute to this project.

### Initialize your development environment

Prerequisites: Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

### Create and run tests

Create tests in the `tests/` directory and then run:

```bash
uv run pytest
```

You can also test the `tap-powerbi-metadata` CLI interface directly using `uv run`:

```bash
uv run tap-powerbi-metadata --help
```

### Testing with [Meltano](https://docs.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano. Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
uv tool install meltano
# Initialize meltano within this directory
cd tap-powerbi-metadata
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-powerbi-metadata --version

# OR run a test ELT pipeline:
meltano run tap-powerbi-metadata target-jsonl
```

### SDK dev guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to develop your own taps and targets.
