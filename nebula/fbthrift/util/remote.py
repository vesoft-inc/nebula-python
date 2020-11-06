"""
PyRemote

Used by PyRemote files generated by
  /thrift/compiler/generate/t_py_generator.cc

Remote.run is the interface used by the generated code.
Based on whether --host or --url is specified as a commandline option,
either a RemoteHostClient or RemoteHttpClient is instantiated to
handle the request.

Additional remote client types (subclasses of RemoteClient) can be
registered with the Remote class to define different ways of specifying a
host or communicating with the host. When registering a new client type,
you can specify the option used to select that type (i.e., url) with the
SELECTOR_OPTIONS attribute, and you can specify additional commandline options
with the CMDLINE_OPTIONS attribute. See the implementations of RemoteHostClient
and RemoteHttpClient for examples.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import json
import os
import pprint
import sys
import traceback

from six.moves.urllib.parse import urlparse
from six import string_types
from nebula.fbthrift import Thrift
from nebula.fbthrift.transport import TTransport, TSocket, TSSLSocket, THttpClient
from nebula.fbthrift.transport.THeaderTransport import THeaderTransport
from nebula.fbthrift.transport.TFuzzyHeaderTransport import TFuzzyHeaderTransport
from nebula.fbthrift.protocol import TBinaryProtocol, TCompactProtocol, \
    TJSONProtocol, THeaderProtocol, TSimpleJSONProtocol


class Function(object):
    """Metadata for a service method"""
    def __init__(self, fn_name, svc_name, return_type, args):
        self.fn_name = fn_name
        self.svc_name = svc_name
        self.return_type = return_type
        self.args = args


def print_functions(functions, service_names, out, local_only=False):
    """Print all the functions available from this thrift service"""
    fns_by_service_name = {svc_name: {} for svc_name in service_names}
    for fn in functions.values():
        fns_by_service_name[fn.svc_name][fn.fn_name] = fn

    svc_names = service_names[0:1] if local_only else \
                    reversed(service_names)
    for svc_name in svc_names:
        out.write('Functions in %s:\n' % (svc_name,))
        for fn_name, fn in sorted(fns_by_service_name[svc_name].items()):
            if fn.return_type is None:
                out.write('  oneway void ')
            else:
                out.write('  %s ' % (fn.return_type,))
            out.write(fn_name + '(')
            out.write(', '.join('%s %s' % (type, name)
                                for type, name, true_type in fn.args))
            out.write(')\n')


format_to_helper = {
    "input": {},
    "output": {},
}
format_to_help_message = {
    "input": {},
    "output": {},
}


def add_format(name, format_type, help_msg=None):
    """
    Decorate function to set it as a handler for the specified format and format_type

    All functions with same format_type must share the same interface/signature.
    In other cases, the signature is allowed to differ.
    """
    lookup_table = format_to_helper[format_type]

    def builder(func):
        if name in lookup_table:
            raise ValueError("Format name '{}' is used twice".format(name))
        lookup_table[name] = func
        if help_msg is not None:
            format_to_help_message[format_type][name] = help_msg
        return func
    return builder


def get_helper_for_format(name, format_type):
    helper = format_to_helper[format_type].get(name)
    if name == "help":
        full_help_message = '\nDetailed help messages:\n\n' + '\n\n'.join(
            '[{}] {}'.format(*x)
            for x in sorted(
                format_to_help_message[format_type].items(),
                key=lambda x: x[0],
            )
        )
        print(
            'List of all formats: {}'.format(
                ', '.join(format_to_helper[format_type].keys())
            ),
            full_help_message if format_to_help_message[format_type] else '',
            file=sys.stderr
        )
        sys.exit(os.EX_USAGE)
    if helper is None:
        sys.stderr.write("Invalid {} format: {}\n".format(format_type, name))
        sys.exit(os.EX_USAGE)
    return helper


@add_format("python", "output")
def __python_output_handler(ret):
    if isinstance(ret, string_types):
        print(ret)
    else:
        pprint.pprint(ret, indent=2)


def __thrift_to_json(x):
    trans = TTransport.TMemoryBuffer()
    proto = TSimpleJSONProtocol.TSimpleJSONProtocol(trans)
    x.write(proto)
    return json.loads(trans.getvalue())


@add_format("json", "output")
def __json_output_handler(ret):
    """
    Python object
    {
        "foo": [
            ThriftStructB(
                x=2
            ),
        ],
        "x": ["%set is nice", 9,8,7, set("blah % blah", 4, 5, 6)],
        "bar": ThriftStructA(
            x=1,
            y="b",
            z=[1,2,3]
        ),
    }

    <=>
    JSON object
    {
        "foo": [
            {"x": 2}
        ],
        "x": ["%set is nice", 9,8,7, ["blah % blah", 4, 5, 6]],
        "bar": {
            "x": 1,
            "y": "b",
            "z": [1,2,3]
        }
    }

    There is no need to handle the type ambiguity between Json dict and
        thrift structs, because pyremote knows what type the services want,
        and we simply try to convert them to that type.

    Also, the exact form of dictionaries produced for Thrift structs may differ
        based across different Thrift versions.
    """
    print(json.dumps(ret, default=__thrift_to_json))


def __eval_arg(arg, thrift_types):
    """Evaluate a commandline argument within the scope of the IF types"""
    code_globals = {}
    code_globals.update(thrift_types)
    # Explicitly compile the code so that it does not inherit our
    # __future__ directives imported above.  In particular this ensures
    # that string literals are not treated as unicode unless explicitly
    # qualified as such.
    code = compile(arg, '<command_line>', 'eval', 0, 1)
    return eval(code, code_globals)


def __preprocess_input(fn, args, ctx):
    if len(args) != len(fn.args):
        sys.stderr.write(('"%s" expects %d arguments (received %d)\n')
                         % (fn.fn_name, len(fn.args), len(args)))
        sys.exit(os.EX_USAGE)

    # Get all custom Thrift types
    return {
        key: getattr(ctx.ttypes, key)
        for key in dir(ctx.ttypes)
    }


@add_format("python", "input", (
    'Evaluate every string in "function_args" using eval() so '
    'that we can support any type of data, unless we already know '
    'the thrift function accepts that argument as a string. In that '
    'case, we simply pass your string without eval().'
))
def __python_natural_input_handler(fn, args, ctx):
    return __python_eval_input_handler(fn, [
        repr(x) if y[2] == 'string' else x
        for x, y in zip(args, fn.args)
    ], ctx)


@add_format("python_eval", "input", (
    'Similar to "python", but we evaluate everything, including strings.'
))
def __python_eval_input_handler(fn, args, ctx):
    thrift_types = __preprocess_input(fn, args, ctx)
    fn_args = []
    for arg in args:
        try:
            value = __eval_arg(arg, thrift_types)
        except Exception:
            traceback.print_exc(file=sys.stderr)
            sys.stderr.write('error parsing argument "%s"' % (arg,))
            sys.exit(os.EX_DATAERR)
        fn_args.append(value)

    return fn_args


@add_format("python_eval_stdin", "input", (
    'Disables the command line option "function_args", and requires '
    'you to pass parameters from stdin. The string you passed in will '
    'be sent to eval(). And it must produce a Python list of objects, '
    'which represents the input argument list to the thrift function.'
))
def __python_stdin_input_handler(fn, args, ctx):
    new_args = json.load(sys.stdin)
    return __python_eval_input_handler(fn, new_args, ctx)


def __args_class_for_function(fn, service_class):
    args_class = getattr(service_class, fn.fn_name + "_args", None)
    if not args_class:
        sys.stderr.write(
            "ERROR: <function name>_args class is unexpected missing. Thrift "
            "may have deprecated its usage. Please re-implement pyremote."
        )
        sys.exit(os.EX_USAGE)
    return args_class


@add_format("json", "input", (
    'Please pass in only one string as "function_args". This string '
    'is a json. Its top level must be a dictionary mapping names of '
    'the thrift function\'s parameters to the value you want to pass '
    'in. Make sure to represent thrift objects using the same format '
    'as generated by pyremote (when using json output format). [Hint: '
    'use this option with a command line tool that can operate on JSONs]'
))
def __json_natural_input_handler(fn, args, ctx):
    if len(args) != 1:
        sys.stderr.write(
            'Error: when using "json" input format, only one cmdline argument '
            'should be used to specify function call arguments. Store arguments '
            'as a json list.'
        )
        sys.exit(os.EX_USAGE)
    partially_decoded = json.loads(args[0])
    if not isinstance(partially_decoded, dict):
        sys.stderr.write(
            "ERROR: Your json input must be a dictionary (of function arguments).\n"
        )
        sys.exit(os.EX_USAGE)
    args_class = __args_class_for_function(fn, ctx.service_class)
    args_obj = args_class()
    args_obj.readFromJson(partially_decoded, is_text=False)
    ans = [getattr(args_obj, arg_name, None) for _, arg_name, _ in fn.args]
    if None in ans:
        sys.stderr.write(
            "ERROR: <function name>_args class is unexpected missing. Thrift "
            "may have deprecated its usage. Please re-implement pyremote."
        )
        sys.exit(os.EX_USAGE)
    return ans


@add_format("json_stdin", "input", (
    'Similar to "json". But this disables the command line option "function_args" '
    'and accepts one json string from stdin.'
))
def __json_stdin_input_handler(fn, args, ctx):
    return __json_natural_input_handler(fn, [sys.stdin.read()], ctx)


def __is_thrift_struct(obj):
    try:
        json.dumps(obj)
        return False
    except BaseException:
        return True


def __get_template_for_struct(struct_type):
    fields = [
        (x[1], x[2], x[3]) for x in
        struct_type.thrift_spec
        if x is not None
    ]
    ans = {}
    for type1, name, type2 in fields:
        if type1 != Thrift.TType.STRUCT:
            ans[name] = "TEMPLATE [TYPE UNKNOWN]"
            continue
        ans[name] = __get_template_for_struct(type2[0])
    return ans


def get_json_template_obj(name, functions, service_class):
    fn = functions.get(name)
    struct = getattr(service_class, name, None)
    if fn is None and struct is None:
        sys.stderr.write("ERROR: unknown structure/function: {}\n".format(name))
        sys.exit(os.EX_USAGE)
    if fn is not None:
        print(
            "Treating", name,
            "as a function. Generating template for its arguments...",
            file=sys.stderr,
        )
        ans_class = __args_class_for_function(fn, service_class)
    elif struct is not None:
        print(
            "Treating", name,
            "as a structure. Generating template for it...",
            file=sys.stderr,
        )
        ans_class = struct
    return __get_template_for_struct(ans_class)


class RemoteClient(object):
    def __init__(self, functions, service_names, service_class,
                 ttypes, print_usage, default_port):
        self.functions = functions
        self.service_names = service_names
        self.service_class = service_class
        self.ttypes = ttypes
        self.print_usage = print_usage
        self.default_port = default_port

    def _exit(self, error_message=None, status=os.EX_USAGE, err_out=sys.stderr):
        """ Report an error, show help information, and exit the program """
        if error_message is not None:
            print("Error: %s" % error_message, file=err_out)

        if status is os.EX_USAGE:
            self.print_usage(err_out)

        if (self.functions is not None and
                status in {os.EX_USAGE, os.EX_CONFIG}):
            print_functions(self.functions, self.service_names, err_out)

        sys.exit(status)

    def _validate_options(self, options):
        """Check option validity and call _exit if there is an error"""
        pass

    def _get_client(self, options):
        """Get the thrift client that will be used to make method calls"""
        raise TypeError("_get_client should be called on "
                        "a subclass of RemoteClient")

    def _close_client(self):
        """After making the method call, do any cleanup work"""
        pass

    def _process_args(self, cmdline_args):
        """Populate instance data using commandline arguments"""
        fn_name = cmdline_args.function_name
        if fn_name not in self.functions:
            self._exit(error_message='Unknown function "%s"' % fn_name,
                       status=os.EX_CONFIG)
        else:
            function = self.functions[fn_name]

        function_args = cmdline_args.input_format(
            function, cmdline_args.function_args, self
        )

        self._validate_options(cmdline_args)
        return function.fn_name, function_args

    def _execute(self, fn_name, fn_args, cmdline_args):
        """Make the requested call.
        Assumes _parse_args() and _process_args() have already been called.
        """
        client = self._get_client(cmdline_args)

        # Call the function
        method = getattr(client, fn_name)
        try:
            ret = method(*fn_args)
        except Thrift.TException as e:
            ret = 'Exception:\n' + str(e)

        cmdline_args.output_format(ret)

        transport = client._iprot.trans
        if isinstance(transport, THeaderTransport):
            response_headers = transport.get_headers()
            if response_headers is not None and len(response_headers) > 0:
                print("Response headers:")
                pprint.pprint(transport.get_headers(), indent=2)

        self._close_client()

    def run(self, cmdline_args):
        fn_name, fn_args = self._process_args(cmdline_args)
        self._execute(fn_name, fn_args, cmdline_args)
        self._exit(status=0)


class RemoteTransportClient(RemoteClient):
    """Abstract class for clients with transport manually opened and closed"""
    CMDLINE_OPTIONS = [
        (
            ('-f', '--framed'),
            {
                'action': 'store_true',
                'default': False,
                'help': 'Use framed transport'
            }
        ), (
            ('-s', '--ssl'),
            {
                'action': 'store_true',
                'default': False,
                'help': 'Use SSL socket'
            }
        ), (
            ('-U', '--unframed'),
            {
                'action': 'store_true',
                'default': False,
                'help': 'Use unframed transport'
            }
        ), (
            ('-j', '--json'),
            {
                'action': 'store_true',
                'default': False,
                'help': 'Use TJSONProtocol'
            }
        ), (
            ('-c', '--compact'),
            {
                'action': 'store_true',
                'default': False,
                'help': 'Use TCompactProtocol'
            }
        ), (
            ('-H', '--headers'),
            {
                'action': 'store',
                'metavar': 'HEADERS_DICT',
                'help':
                'Python code to eval() into a dict of write headers',
            }
        ),
    ]

    def _get_client_by_transport(self, options, transport, socket=None):
        # Create the protocol and client
        if options.json:
            protocol = TJSONProtocol.TJSONProtocol(transport)
        elif options.compact:
            protocol = TCompactProtocol.TCompactProtocol(transport)

        # No explicit option about protocol is specified. Try to infer.
        elif options.framed or options.unframed:
            protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)

        elif socket is not None:
            # If json, compact, framed, and unframed are not specified,
            # THeaderProtocol is the default. Create a protocol using either
            # fuzzy or non-fuzzy transport depending on if options.fuzz is set.
            if options.fuzz is not None:
                transport = TFuzzyHeaderTransport(
                    socket, fuzz_fields=options.fuzz, verbose=True)
            else:
                transport = THeaderTransport(socket)
                if options.headers is not None:
                    try:
                        parsed_headers = eval(options.headers)
                    except Exception:
                        self._exit(
                            error_message='Request headers (--headers) argument'
                                          ' failed eval')
                    if not isinstance(parsed_headers, dict):
                        self._exit(
                            error_message='Request headers (--headers) argument'
                                          ' must evaluate to a dict')
                    for header_name, header_value in parsed_headers.items():
                        transport.set_header(header_name, header_value)
            protocol = THeaderProtocol.THeaderProtocol(transport)
        else:
            self._exit(error_message=('No valid protocol '
                                      'specified for %s' % (type(self))),
                       status=os.EX_USAGE)

        transport.open()
        self._transport = transport

        client = self.service_class.Client(protocol)

        return client

    def close_client(self):
        self._transport.close()

    def _validate_options(self, options):
        super(RemoteTransportClient, self)._validate_options(options)
        if options.framed and options.unframed:
            self._exit(error_message='cannot specify both '
                       '--framed and --unframed')

    def _parse_host_port(self, value, default_port):
        parts = value.rsplit(':', 1)
        if len(parts) == 1:
            return (parts[0], default_port)
        try:
            port = int(parts[1])
        except ValueError:
            raise ValueError('invalid port: ' + parts[1])
        return (parts[0], port)


class RemoteHostClient(RemoteTransportClient):
    SELECTOR_OPTIONS = 'host'
    CMDLINE_OPTIONS = list(RemoteTransportClient.CMDLINE_OPTIONS) + [(
        ('-h', '--host'),
        {
            'action': 'store',
            'metavar': 'HOST[:PORT]',
            'help': 'The host and port to connect to'
        }
    ), (
        ('-F', '--fuzz'),
        {
            'type': str,
            'nargs': '*',
            'default': None,
            'help': ('Use TFuzzyHeaderTransport to send a fuzzed message for '
                     'testing thrift transport. Optionally include a list of '
                     'message field names to fuzz after this flag. Fields: ' +
                     ', '.join(TFuzzyHeaderTransport.fuzzable_fields))
        }
    )]

    def _validate_options(self, options):
        super(RemoteHostClient, self)._validate_options(options)
        if (options.fuzz is not None and
            any([options.framed, options.unframed,
                 options.json, options.compact])):
            self._exit(error_message=('Transport fuzzing only supported for '
                                      'THeaderTransport (no framed, unframed, '
                                      'json, or compact.)'),
                       status=os.EX_USAGE)

    def _get_client(self, options):
        host, port = self._parse_host_port(options.host, self.default_port)
        socket = (TSSLSocket.TSSLSocket(host, port) if options.ssl
                  else TSocket.TSocket(host, port))
        if options.framed:
            transport = TTransport.TFramedTransport(socket)
        else:
            transport = TTransport.TBufferedTransport(socket)
        return self._get_client_by_transport(options, transport, socket=socket)


class RemoteHttpClient(RemoteTransportClient):
    SELECTOR_OPTIONS = 'url'
    CMDLINE_OPTIONS = list(RemoteTransportClient.CMDLINE_OPTIONS) + [(
        ('-u', '--url'),
        {
            'action': 'store',
            'help': 'The URL to connect to, for HTTP transport'
        }
    )]

    def _get_client(self, options):
        url = urlparse(options.url)
        host, port = self._parse_host_port(url[1], 80)
        transport = THttpClient.THttpClient(options.url)
        return self._get_client_by_transport(options, transport)

    def _validate_options(self, options):
        """Check if there are any option inconsistencies, and exit if so"""
        super(RemoteHttpClient, self)._validate_options(options)
        if not any([options.unframed, options.json]):
            self._exit(error_message='can only specify --url with '
                       '--unframed or --json')


class RemoteUNIXDomainClient(RemoteTransportClient):
    SELECTOR_OPTIONS = 'path'
    CMDLINE_OPTIONS = list(RemoteTransportClient.CMDLINE_OPTIONS) + [(
        ('-p', '--path'),
        {
            'action': 'store',
            'help': 'The path of the socket to use'
        }
    )]

    def _get_client(self, options):
        socket = TSocket.TSocket(unix_socket=options.path)
        if options.framed:
            transport = TTransport.TFramedTransport(socket)
        else:
            transport = TTransport.TBufferedTransport(socket)
        return self._get_client_by_transport(options, transport, socket=socket)


class Namespace(object):
    def __init__(self, attrs=None):
        if attrs is not None:
            self.__dict__.update(attrs)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value


class Remote(object):
    __client_types = set()
    __occupied_args = {}
    __parser = argparse.ArgumentParser(add_help=False)

    @classmethod
    def register_cmdline_options(cls, cmdline_options):
        for args, kwargs in cmdline_options:
            is_repeated = False
            for arg in args:
                if arg in cls.__occupied_args:
                    if cls.__occupied_args[arg] != kwargs:
                        raise ValueError('Redefinition of {}'.format(arg))
                    is_repeated = True
            if is_repeated:
                continue
            cls.__occupied_args.update({x: kwargs for x in args})
            cls.__parser.add_argument(*args, **kwargs)

    @classmethod
    def register_client_type(cls, client_type):
        if not issubclass(client_type, RemoteClient):
            raise TypeError(('Remote client must be of type RemoteClient. '
                             'Got type %s.' % client_type.__name__))
        if client_type is RemoteClient:
            raise TypeError(('Remote client must be a strict subclass '
                             'of RemoteClient.'))
        if not hasattr(client_type, 'SELECTOR_OPTIONS'):
            raise AttributeError(('Remote client must have a '
                                  'SELECTOR_OPTIONS field.'))

        cls.__client_types.add(client_type)
        cls.register_cmdline_options(client_type.CMDLINE_OPTIONS)

    @classmethod
    def _exit_usage_error(cls, message):
        sys.stderr.write('ERROR: ' + message + '\n')
        cls.__parser.print_help(sys.stderr)
        sys.exit(os.EX_USAGE)

    @classmethod
    def _get_client_type(cls, options):
        matching_types = [ct for ct in cls.__client_types if
                          getattr(options, ct.SELECTOR_OPTIONS) is not None]
        if len(matching_types) != 1:
            cls._exit_usage_error('Must specify exactly one of [%s]' % (
                ', '.join('--%s' % ct.SELECTOR_OPTIONS
                          for ct in cls.__client_types)))
        else:
            return matching_types[0]

    @classmethod
    def _parse_cmdline_options(cls, argv):
        cls.register_cmdline_options((
            (
                ('-ifmt', '--input-format'),
                {
                    'action': 'store',
                    'default': 'python',
                    'type': lambda x: get_helper_for_format(x, "input"),
                    'help': (
                        'Change the format for function_args. Generally speaking, '
                        'there are two main formats: python_* and json_*. Defaults '
                        'to "python". Use -ifmt help for entire list of available '
                        'formats.'
                    ),
                },
            ),
            (
                ('-ofmt', '--output-format', ),
                {
                    'action': 'store',
                    'default': 'python',
                    'type': lambda x: get_helper_for_format(x, "output"),
                    'help': (
                        'Change the output format for the return value. The '
                        'default is "python", which direclty prints out strings '
                        'and pprint() other types. Available formats: {}.'
                    ).format(','.join(format_to_helper["output"].keys()))
                },
            ),
            (
                ('--help', ),
                {'action': 'help'},
            ),
            (
                ('-la', '--list-all-functions'),
                {'action': 'store_true'},
            ),
            (
                ('-l', '--list-functions', ),
                {'action': 'store_true'},
            ),
            (
                ('-g', '--generate-template'),
                {
                    'action': 'store',
                    'metavar': 'THRIFT_STRUCT_OR_FUNCTION_NAME',
                    'help': (
                        'Generate a template for a thrift struct, OR, arguments of '
                        'a function call. Currently it supports only json format. '
                        'No need to specify function_name.'
                    )
                }
            ),
            (
                ('function_name', ),
                {'nargs': '?', 'help': 'Name of the remote function to call'},
            ),
            (
                ('function_args', ),
                {'nargs': '*', 'help': (
                    'Arguments for the remote function. Look at --input-format '
                    'for more details.'
                )},
            ),
        ))
        try:
            return cls.__parser.parse_args(argv[1:])
        except BaseException:
            sys.exit(os.EX_USAGE)

    @classmethod
    def run(cls, functions, service_names, service_class,
            ttypes, argv, default_port=9090):
        args = cls._parse_cmdline_options(argv)
        conflicts = [x for x in [
            "list_all_functions",
            "list_functions",
            "generate_template",
        ] if getattr(args, x)]
        if len(conflicts) > 1:
            cls._exit_usage_error(
                'Please do not specify all of {} at once.'.format(
                    ','.join(conflicts)
                )
            )
        if args.list_all_functions:
            print_functions(functions, service_names, sys.stdout, local_only=False)
            return
        if args.list_functions:
            print_functions(functions, service_names, sys.stdout, local_only=True)
            return
        if args.function_name is None:
            cls._exit_usage_error('Please specify function_name.')
        if args.generate_template:
            ans = get_json_template_obj(
                args.generate_template, functions, service_class
            )
            print(json.dumps(ans))
            return
        client_type = cls._get_client_type(args)
        client = client_type(functions, service_names, service_class, ttypes,
                             cls.__parser.print_help, default_port)
        client.run(args)


Remote.register_client_type(RemoteHostClient)
Remote.register_client_type(RemoteHttpClient)
Remote.register_client_type(RemoteUNIXDomainClient)
