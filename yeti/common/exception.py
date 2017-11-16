#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'


class BaseException(Exception):
    http_code = 500
    return_value = None
    message = 'Internal Server Error.'

    def __init__(self, *args, **kwargs):
        self.message %= args
        self.return_value = kwargs.get('return_value')


class SubnetIPCannotRelease(BaseException):
    message = 'ALL ips in subnet cannot all available, subnet is %s.'


class ExistSubnetInNetowrk(BaseException):
    message = 'There are subnets in network, network is %s.'


class NotSupportedPurpose(BaseException):
    message = 'Not supported purpose of %s.'


class NoParentNetID(BaseException):
    message = 'No parent network %s.'


class NoAvailableIP(BaseException):
    message = 'No available ip of subnet %s.'


class HostnameExist(BaseException):
    message = 'Hostname of %s exists.'


class NoElasticIP(BaseException):
    message = 'Not enough elastic ip quota.'


class NoSecGroup(BaseException):
    message = 'Not enough secgroup quota.'


class SecGroupNameExist(BaseException):
    message = 'SecGroupName of %s exists.'


class LoginFailed(BaseException):
    message = 'Login failed.'
    http_code = 401


class PermissionDenied(BaseException):
    message = 'Permission denied. %s'
    http_code = 401


class RoleDenied(BaseException):
    message = 'Role denied. %s'
    http_code = 401


class EIPUnavailable(BaseException):
    message = 'Elastic IP unavailable.'


class BadParameter(BaseException):
    message = 'Bad parameter is %s.'


class NoRequiredParameter(BaseException):
    message = 'No required parameter %s.'


class NotFound(BaseException):
    http_code = 404


class FailToCreateServer(BaseException):
    message = 'Fail to create server %s, reason is %s'


class APIAuthFailed(BaseException):
    http_code = 401
    message = 'Fail to authenticate the request, the api access key is %s.'


class MonitorAPIError(BaseException):
    def __init__(self, http_code):
        self.http_code = http_code

    message = 'Fail to call monitor api.'


class ServiceActionLimit(BaseException):
    http_code = 401
    message = 'Exceed the max limits of %s.'


class TokenExpired(BaseException):
    http_code = 403
    message = 'Token expired.'


class PolicyCondition(BaseException):
    pass


class MonitorTSDBQueryError(BaseException):
    message = 'Fail to query monitor tsdb, reason is %s'


class yetiError(BaseException):
    message = 'Fail to query aws bill system, reason is %s'
