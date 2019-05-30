# User settings

The `users` section of the `user.xml` aggregates settings for some user.

Structure of the `users` section:

```
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <password></password>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>

        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                <table_name>
            </database_name>
        </databases>
    </user_name>
</users>
```

### user_name/password

Password could be specified in plaintext or in SHA256 (in hex format). If you want to specify password in plaintext (not recommended), place it in 'password' element. For example, `<password>qwerty</password>`. Password could be empty.

If you want to specify SHA256 hash of a password, place it in `password_sha256_hex` element. For example, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

To generate decent password, in the command prompt perform the command:

```
PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
```

The first line of the result is the password. The second line is the corresponding SHA256 hash.


### user_name/networks

List of networks which the user can connect to ClickHouse server from.

Each element of list has one of the following forms:

- `<ip>` — IP-address or a network mask. Examples: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.
- `<host>` — Hostname. Example: `server01.yandex.ru`.

    To check access, DNS query is performed, and all received addresses compared to peer address.

- `<host_regexp>` — Regular expression for host names. Example, `^server\d\d-\d\d-\d\.yandex\.ru$`

    To check access, DNS PTR query is performed for peer address and then regexp is applied. Then, for result of PTR query, another DNS query is performed and all received addresses compared to peer address. Strongly recommended that regexp is ends with $

All results of DNS requests are cached till server restart.

**Examples**

To open access for user from any network, specify:

```
<ip>::/0</ip>
```

To open access only from localhost, specify:

```
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

[Settings profile](settings_profiles.md).

### user_name/quota

???

### user_name/databases

In this section you can choouse which rows can be returned by `SELECT` query. This feature implements a kind of Row-Level Security in ClickHouse.

**Example**

The following configurations sets that the user `user1` can see only the rows of `table1` as a result of `SELECT` query where the value of field `id` equals to `1000`.

```
<user1>
    <databases>
        <database_name>
            <table1>
                <filter>id = 1000</filter>
            </table1>
        </database_name>
    </databases>
</user1>
```

The filter is usually an expression with comparison and logical operators. You can use any conditional expression for the filter. The filtering is incompatible with `PREWHERE` operations and disables `WHERE→PREWHERE` optimization.
