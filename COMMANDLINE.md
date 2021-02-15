Neocortix provides a command-line tool for launching and managing Scalable Compute instances. It is tested mostly in Linux, but is written in standard Python 3, and also works on Windows 10, WIndows Server 2019, and MacOS.

## Prerequisites

First, you need to set up a Neocortix Cloud Services account. For instructions, see https://neocortix.com/tutorials-account.

The ncscli package requires python (3.6, 3.7, 3.8, or 3.9) and pip for python 3. If you don't have python 3 installed, see https://www.python.org/downloads/. If you don't have pip installed for python 3, see https://pip.pypa.io/en/stable/installing/.

### Notes for Windows Users

We recommend using *Windows Subsystem for Linux (WSL) 2* with an Ubuntu 20 or Debian 10 distribution on Windows 10. In that envronment, you can use our examples exactly as written. If you prefer not to use WSL, recent testing has shown that the best results can be achieved by installing python using the 64-bit installer from python.org. If you have used a different method of installation (such as the Windows Store, or Anaconda), you may have to work though some issues.

Windows versions before Windows 10, version 1909, are not supported.

In some environments., you may have to use a `python -m ncscli.ncs` command, rather than the `ncs.py` command that we show in examples.

### Notes for Ubuntu and Debian Users

Installing pip using the official `python.org` instructions can be trouble-prone. You can do it more easily using the following commands

```shell
sudo apt update
sudo apt install python3-pip
# for best results, update pip like this
python3 -m pip install --upgrade pip
```



## Installing the command-line tool and library

Install our python-based package using the following command. On some systems, you may need to use the `python` or `py` command, instead of python3.
```shell
python3 -m pip install ncscli
```
The command-line tool is `ncs.py`, in the ncscli package. To get basic reference information about it, use
```shell
ncs.py --help
```


## Basic walk-through

Before creating any instances via the command line, you need to sign up for a Neocortix Cloud Services account and create an **authorization token**. To create a token, browse to https://cloud.neocortix.com/profile/api, make sure the "Auth Tokens enabled" toggle is turned on, and then click "New token". That creates a token as a long pseudorandom string. Copy that string for use below wherever you see *YourAuthTokenHere*.


Create and start some instances using the **launch** subcommand. In this example, we create 2 instances.
```shell
ncs.py sc launch --authToken YourAuthTokenHere --count 2 --showPasswords --json --encryptFiles false
```


The launch will display some progress messages to stderr, and print information about the new instances to stdout. The progress messages will look something like this.
```
2021/02/03 01:18:29 INFO ncs launchScInstancesAsync job request returned (200) {"id":"6b8675b8-ecc8-42e1-b93e-ffc97047f4d7"}
2021/02/03 01:18:29 INFO ncs launchScInstancesAsync waiting for server (0 instances allocated)
2021/02/03 01:18:39 INFO ncs doCmdLaunch allocated 2 instances
2021/02/03 01:18:40 INFO ncs doCmdLaunch 0 instance(s) launched so far; Counter({'starting': 2})
2021/02/03 01:18:50 INFO ncs doCmdLaunch 1 instance(s) launched so far; Counter({'starting': 1, 'started': 1})
2021/02/03 01:19:00 INFO ncs doCmdLaunch 2 instance(s) launched so far; Counter({'started': 1})
2021/02/03 01:19:00 INFO ncs doCmdLaunch started 2 Instances; Counter({'started': 1})
2021/02/03 01:19:00 INFO ncs doCmdLaunch querying for device-info
```
The final output to stdout will look something like this (but, even more verbose). It is a json array of instance description objects. You may want to redirect the stdout to a file, for later use.
```json
[
{"state": "started", "started-at": "2021-02-03T01:18:55.495Z", "job": "6b8675b8-ecc8-42e1-b93e-ffc97047f4d7", "encrypt-files": false, "device-location": {"latitude": 33.8206, "longitude": -84.0229, "display-name": "Snellville, GA, United States", "country": "United States", "country-code": "US", "area": "GA", "locality": "Snellville", "tz": {"id": "America/New_York"}}, "replaced-devices": [], "app-version": {"name": "2.1.11", "code": 2111}, "cpu": {"arch": "aarch64", "cores": [{"vendor": "qualcomm", "family": "Kryo", "freq": 1593600000}, {"vendor": "qualcomm", "family": "Kryo", "freq": 1593600000}, {"vendor": "qualcomm", "family": "Kryo", "freq": 2150400000}, {"vendor": "qualcomm", "family": "Kryo", "freq": 2150400000}]}, "ram": {"total": 3903582208}, "storage": {"total": 24622886912, "free": 16197738496}, "dpr": 25.4592, "category": "silver", "progress": "Installing a 'debian' payload...\n## Install: debian\n\u001b[1G\u001b[2KInstalling...\u001b[1G\u001b[2KInstalled:           19129 files\nStarting an SSH server... DONE\nOpening a tunnel... DONE\nStarting a network buzzer... DONE\nSC instance launched\nVerifying instance... OK\n", "events": [{"timestamp": "2021-02-03T01:18:33.753Z", "category": "instance", "event": "launching"}, {"timestamp": "2021-02-03T01:18:50.228Z", "category": "instance", "event": "launched"}], "hostname": "android-eprminttunbuvqdt.p.cloud.neocortix.com", "ssh": {"host": "android-eprminttunbuvqdt.p.cloud.neocortix.com", "port": 41505, "user": "root", "password": "0F1IT1ZlVwj5j9fMvIsRbGgMhQk9utbt", "host-keys": {...}}, "instanceId": "2e9a6d6b-729f-4909-bcb4-6cc76f96fe1e"}
, {"state": "started", "started-at": "2021-02-03T01:18:48.782Z", "job": "6b8675b8-ecc8-42e1-b93e-ffc97047f4d7", "encrypt-files": false, "device-location": {"latitude": 32.5892, "longitude": -96.944, "display-name": "Cedar Hill, TX, United States", "country": "United States", "country-code": "US", "area": "TX", "locality": "Cedar Hill", "tz": {"id": "America/Chicago"}}, "replaced-devices": [], "app-version": {"name": "2.1.11", "code": 2111}, "cpu": {"arch": "aarch64", "cores": [{"vendor": "arm", "family": "Cortex-A55", "freq": 1766400000}, {"vendor": "arm", "family": "Cortex-A55", "freq": 1766400000}, {"vendor": "arm", "family": "Cortex-A55", "freq": 1766400000}, {"vendor": "arm", "family": "Cortex-A55", "freq": 1766400000}, {"vendor": "arm", "family": "Cortex-A75", "freq": 2803200000}, {"vendor": "arm", "family": "Cortex-A75", "freq": 2803200000}, {"vendor": "arm", "family": "Cortex-A75", "freq": 2803200000}, {"vendor": "arm", "family": "Cortex-A75", "freq": 2803200000}]}, "ram": {"total": 3826167808}, "storage": {"total": 49018085376, "free": 42215153664}, "dpr": 52.46976, "category": "gold", "progress": "Installing a 'debian' payload...\n## Install: debian\n\u001b[1G\u001b[2KInstalling...\u001b[1G\u001b[2KInstalled:           19129 files\nStarting an SSH server... DONE\nOpening a tunnel... DONE\nStarting a network buzzer... DONE\nSC instance launched\nVerifying instance... OK\n", "events": [{"timestamp": "2021-02-03T01:18:31.864Z", "category": "instance", "event": "launching"}, {"timestamp": "2021-02-03T01:18:45.415Z", "category": "instance", "event": "launched"}], "hostname": "android-kscjvwtc7tsyuxz5.p.cloud.neocortix.com", "ssh": {"host": "android-kscjvwtc7tsyuxz5.p.cloud.neocortix.com", "port": 35045, "user": "root", "password": "xjk4EHicYyNBfnPKDemEIU2wu3m8Tklf", "host-keys": {...}}, "instanceId": "6bf9e9f1-c7a1-4605-b4e1-71a4c7da6ca9"}
]
```

That's all you need to do. At this point, all instances in the "started" state are running a minimal installation of the Debian linux OS, with an ssh daemon listening on the listed port.

If you want to log in to one of the instances, take the "port", "user" and "host" fields from an output line and use it in an ssh command like the following. The instance will ask for an ssh password; use the one from the corresponding output line from the launch.
```shell
ssh -p <port> <user>@<host>
```

For example (this will not work for you because this example instance doesn't exist)
```shell
ssh -p 36756 root@android-ht4pgxtudv1lc74j.p.cloud.neocortix.com
```


To **list** all existing instances allocated to you, use the list subcommand like this.
```shell
ncs.py sc list --authToken YourAuthTokenHere
```
Output
```
c08e188d-869d-4af3-8c93-6d9a51a5e19f,started,40963,us1.ssh.cloud.neocortix.com,*
8b7c7731-2df4-4e31-b502-b80f1b087338,started,43385,eu1.ssh.cloud.neocortix.com,*
```
In that example, we left out --showPasswords and --json arguments, just to show what the less verbose output looks like. The short form output is comma-delimited and contains the fields instanceId, state, port, host, password, and jobId. 

To stop an instance and delete it, use the **terminate** subcommand, like this. You can get the instanceId from the output of a launch or list command.
```shell
ncs.py sc terminate --authToken YourAuthTokenHere  --instanceId YourInstanceIdHere
```

## Other ways to pass the Auth Token
You may wish to avoid passing your auth token on the command line, for security reasons.

Another way to pass it is by setting an environment variable. The ncs command looks for a variable 'NCS_AUTH_TOKEN' if the --authToken argument is not provided.

Yet another way to pass the auth token, or any command-line argument, is to create a parameter file and then to pass an @-reference to it on the command line.

Specifically, do these steps one time, as a setup:
1. create a file called myNcsAuthToken (or any name that makes sense to you)
2. insert just one line into the file, containing --authToken=YourAuthTokenHere (no spaces)
3. set the ownership and permissions on the file, as appropriate for your situation

Then, you can use a reference to that file each time you run an ncs command, like this.
```shell
ncs.py sc list @myNcsAuthToken
```


## Using SSH Client Keys

When launching an instance, NCS can insert a public key into a user's authorized_keys file on the instance, if desired. This enables you, or a shell script, to log into instances without providing a password.

As a prerequisite, you should have at least one SSH client key uploaded to your Neocortix Cloud Services account. Only public keys are uploaded. If you have never used ssh logins from your workstation, you will need to run `ssh-keygen -t rsa` to create a keypair before uploading.

To upload a public key, first browse to https://cloud.neocortix.com/profile/ssh-keys. Then, open your ~/.ssh/id_rsa.pub file and copy its contents. Then, paste the key contents into the "Key" text box on the ssh-keys page and enter a name for this key in the "Title" text box. Finally, click "Add key" to upload the public key. When launching instances, you will use this title in a --sshClientKeyName argument.

To cause the public key to get inserted onto instances as they are launched, use the --sshClientKeyName option when launching, as in the following example. Use a name you assigned to a key when you uploaded it.

```shell
ncs.py sc launch --sshClientKeyName YourClientKeyNameHere --encryptFiles false
``` 

Having done that, you will be able to ssh into the instance(s) without providing a password.

## More Details

### Output formats

For the *launch* and *list* subcommands, use --json to get output that is easy to parse in most programming languages. If you do not specify --json, the tool produces less-verbose output which is comma-delimited and subject to change in future versions.

### Regions
The *launch* subcommand accepts one or more geographic region identifiers with the --region argument. If you omit the --region argument, the system will pick regions arbitrarily. If you specify one region, it will create all instances in that region. If you specify multiple regions, it will choose among the specified regions arbitrarily.

Here is a list of regions that may be available: 'asia', 'europe', 'middle-east', 'north-america', 'oceania', 'russia-ukraine-belarus', 'usa'. In the future, we expect to add Africa, South America, and others to the list, covering the entire world.

### Specifying Device Requirements

When launching instances, you can specify minimum requirements for the devices you want to use. The main criteria are "ram" (total bytes ram on device), "storage" (available bytes of file-storage space), and "dpr" (our device performance rating). To specify these, pass a --filter argument with a json-format string containing the criteria and values you need, as in this example. (Be sure to use single-quotes around the json string, which contains its own balanced quotes.)
```
ncs.py sc launch --count 2 --encryptFiles false --filter '{"dpr": ">=48", "ram": ">=2800000000", "storage": ">=2000000000"}'
```

We calculate the Device Performance Rating of a device based on its device properties, including the number of CPU cores, clock speeds, and core types (e.g. ARM A53, A57, etc.). A device with one A7 core at 1GHz clock rate has a Device Performance Rating of 1; this device would not meet our minimum performance requirements because it has less than 2 cores and a DPR less than 11. A device with 4 A53 cores at 1.5GHz and 4 A57 cores at 2.1GHz has a Device Performance Rating of 39, and so on. DPR ratings for many device types are shown at https://neocortix.com/device-requirements



### Encryption

Neocortix Cloud instances can transparently encrypt all files in your containers. When launching instances an --encryptFiles argument is required. Use `--encryptFiles true` or `--encryptFiles false`. Some operations will be slightly faster if encryption is turned off.


### Specific Instances or ALL Instances
The *list* and *terminate* subcommands can accept one or more instance IDs with the --instanceId argument. You can also pass --instanceId ALL to list or terminate all instances allocated to you. If you omit --instanceId for the list subcommand, it will list all. If you omit --instanceId for the terminate subcommand, it will return error.

### Instance passwords
Instance passwords are  needed only if you are *not* using public-private key-pairs for ssh client authentication. Key-pairs are generally recommended.

To see instance passwords, pass --showPasswords to the *list* or *launch* subcommand. When not using --showPasswords, a single asterisk is shown where the password field otherwise would be.

### Deleting all instances from a Launch

If you have launched many instances from a single launch command, you may want to terminate them all at once, without specifying each one on a command line. You can do this using the --jobId argument to `ncs.py`. There are 2 ways to get the id of the job.

One way is to look at the INFO output from the `ncs.py` launch command (which goes to stderr). Early in the output, you will see a line like the following. Copy the value of the "id" field to use in a terminate command.
```
2021/02/02 21:51:09 INFO ncs launchScInstancesAsync job request returned (200) {"id":"d0303fa7-5cb7-47e5-9098-dc5c8d11e137"}
```

If that info has scrolled by, or you want to do this programmatcially, look in the json output of your launch command (or a list command). There you will see a "job" field. Copy its value to use in a terminate command.
```
{"state": "started", "started-at": "2021-02-02T21:51:33.630Z", "job": "d0303fa7-5cb7-47e5-9098-dc5c8d11e137", "encrypt-files": false, ...
```

Either way, use a terminate command specifying --jobId like this
```
ncs.py sc terminate --jobId d0303fa7-5cb7-47e5-9098-dc5c8d11e137
```
