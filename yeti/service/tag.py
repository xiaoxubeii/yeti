#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yeti.service import BaseService
from yeti.db.dao import TagDAO
from yeti.db.transaction import transaction
import datetime


class Tag(BaseService):
    dao_class = TagDAO

    def get(self, id):
        pass

    def list(self, context, need_policy=False, clause=None, search=None, is_page=False, **kwargs):

        result = super(Tag, self).list(context=context, need_policy=need_policy, clause=clause, search=search,
                                       is_page=is_page, **kwargs)

        return result

    def create(self, context, entity, need_policy=False):

        tags = entity.get('tags')
        if tags:
            tag = self._make_tag_data(entity)
            if tag.get("resource_id") and tag.get("resource_ins_id"):
                exist_tag = self.dao.filter(None, resource_id=tag.get("resource_id"),
                                            resource_ins_id=tag.get("resourceins_id"),
                                            resource_type=tag["resource_type"]).first()
            elif tag.get("resource_id"):
                exist_tag = self.dao.filter(None, resource_id=tag.get("resource_id"),
                                            resource_type=tag["resource_type"]).first()
            else:
                exist_tag = self.dao.filter(None, resource_ins_id=tag.get("resource_ins_id"),
                                            resource_type=tag["resource_type"]).first()
            with transaction():
                if exist_tag:
                    self.dao.delete(None, id=exist_tag.id)
                new_tag = super(Tag, self).create(None, tag)
                self.rebuild_tag_view()
                return new_tag

    def update(self, context, data, need_policy=False):
        tags = data.get('tags')
        if tags:
            tag = self._make_tag_data(data)

            with transaction():
                data = super(Tag, self).update(context, data['tag_id'], need_policy=need_policy, **tag)
                self.rebuild_tag_view()
                return data

    def delete(self, context, id, need_policy=False):
        kwargs = {'deleted': 1, 'deleted_at': datetime.datetime.now()}
        super(Tag, self).update(context, id, need_policy=need_policy, **kwargs)
        self.rebuild_tag_view()

    def _make_tag_data(self, data):
        resource_id = data.get('resource_id', None)
        ins_id = data.get('instance_id', None)
        resource_type = data.get('resource_type')

        tag = {
            "resource_id": resource_id,
            "resource_type": resource_type,
            "tags": data.get('tags'),
            'resource_ins_id': ins_id
        }
        return tag

    def rebuild_tag_view(self):
        self.dao.rebuild_tag_view()

    def get_val_bykey(self, key):
        tags = self.dao.list_tags()
        return [t.values()[0] for t in tags if t and t.keys()[0] == key]

    def get_vals_bykey(self, keys):
        return {k: self.get_val_bykey(k) for k in keys}
