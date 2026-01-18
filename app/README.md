
# Exchange Simulator Application
For NO SSL remove --secure flag
## Running the Application

The Exchange Simulator must be started from its application directory.

### Startup Instructions

1. Navigate to the application directory:
```bash
cd ./app
```

2. Start the application:
```bash
java -jar exchange-simulator-0.0.1-SNAPSHOT.jar
```

**Important**: The `java -jar` command must be executed from within the `app` working directory for the application to function correctly.

## Access Information

- **Port**: 8080
- **URL**: `http://<server-address>:8080`

## Configuration

User credentials and authentication settings are defined in `application.yaml`. Modify this file to add, remove, or update user access.

### application.yaml Location
```
app/application.yaml
```

## Troubleshooting

If the application fails to start:
- Verify you are in the correct working directory (`app/`)
- Ensure `application.yaml` exists in the current directory
- Check that port 8080 is not already in use: `sudo netstat -tlnp | grep 8080`
- Verify Java is installed: `java -version`


