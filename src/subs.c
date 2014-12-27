/*
Copyright (c) 2010-2013 Roger Light <roger@atchoo.org>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
3. Neither the name of mosquitto nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/

/* A note on matching topic subscriptions.
 *
 * Topics can be up to 32767 characters in length. The / character is used as a
 * hierarchy delimiter. Messages are published to a particular topic.
 * Clients may subscribe to particular topics directly, but may also use
 * wildcards in subscriptions.  The + and # characters are used as wildcards.
 * The # wildcard can be used at the end of a subscription only, and is a
 * wildcard for the level of hierarchy at which it is placed and all subsequent
 * levels.
 * The + wildcard may be used at any point within the subscription and is a
 * wildcard for only the level of hierarchy at which it is placed.
 * Neither wildcard may be used as part of a substring.
 * Valid:
 * 	a/b/+
 * 	a/+/c
 * 	a/#
 * 	a/b/#
 * 	#
 * 	+/b/c
 * 	+/+/+
 * Invalid:
 *	a/#/c
 *	a+/b/c
 * Valid but non-matching:
 *	a/b
 *	a/+
 *	+/b
 *	b/c/a
 *	a/b/d
 */

#include <config.h>

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include <mosquitto_broker.h>
#include <memory_mosq.h>
#include <util_mosq.h>
#include <errno.h>
#include "mqtt3_protocol.h"

struct _sub_token {
	struct _sub_token *next;
	char *topic;
};

static int _subs_process(struct mosquitto_db *db, struct _mosquitto_subhier *hier, const char *source_id, const char *topic, int qos, int retain, struct mosquitto_msg_store *stored, bool set_retain, bool BigFile)
{
	int rc = 0;
	int rc2;
	int client_qos, msg_qos;
	uint16_t mid;
	struct _mosquitto_subleaf *leaf;
	bool client_retain;
        // if BigFile is true, it make sense, else useless
        int packetlen;  
	struct _mosquitto_packet *packet = NULL;
        ssize_t write_length;

        leaf = hier->subs;
	if(retain && set_retain){
#ifdef WITH_PERSISTENCE
		if(strncmp(topic, "$SYS", 4)){ //证明这不是系统消息
			/* Retained messages count as a persistence change, but only if
			 * they aren't for $SYS. */
			db->persistence_changes++;
		}
#endif
		if(hier->retained){ //如果非空，证明这里有上次存储的retain的消息，现在有新的消息需要retain,则替换旧的消息，故要做下面的减法
			hier->retained->ref_count--;
			/* FIXME - it would be nice to be able to remove the message from the store at this point if ref_count == 0 */
			db->retained_count--;
                  //printf("hier->retained is not null,hier->retained->ref_count:%d,db->retained_count:%d\n", hier->retained->ref_count, db->retained_count);
		}
		if(stored->msg.payloadlen){
			hier->retained = stored;
			hier->retained->ref_count++;
			db->retained_count++;
     //       printf("stored->msg.payloadlen is not null,topic:%shier->retained->ref_count:%d,db->retained_count:%d\n",stored->msg.topic,  hier->retained->ref_count, db->retained_count);
		}else{
			hier->retained = NULL;
      //           printf("stored->msg.payloadlen is null\n");
		}
	}

        if(BigFile){//先插入树结构中
            if(hier->large_file){
                hier->large_file->ref_count--;
            }
            if(stored->msg.payloadlen){
                hier->large_file->ref_count++;
                hier->large_file = stored;
                //初始化发送包信息
                packetlen = 2 + strlen(stored->msg.topic) + stored->msg.payloadlen;
                packet = _mosquitto_calloc(1, sizeof(struct _mosquitto_packet));
                if(!packet)
                    return MOSQ_ERR_NOMEM;
                packet->mid = 0;
                packet->command = PUBLISH | ((false&0x1)<<3) | (0<<1) | 0;//false 对应 dup ,0 对应 qos  , 0对应 retain
                packet->remaining_length = packetlen;
                rc = _mosquitto_packet_alloc(packet);
                if(rc){
                    _mosquitto_free(packet);
                    return rc;
                }
                 _mosquitto_write_string(packet, stored->msg.topic, strlen(stored->msg.topic));
                 /* Payload */
                 if( stored->msg.payloadlen){
                    _mosquitto_write_bytes(packet, stored->msg.payload, stored->msg.payloadlen);
                 }
                 packet->pos = 0;
                 packet->next = NULL;
                 packet->to_process = packet->packet_length;
                 
            }else{
                hier->large_file = NULL;
            }
        }        

	while(source_id && leaf){ //若没有订阅者，则这里不会执行
		if(leaf->context->is_bridge && !strcmp(leaf->context->id, source_id)){
			leaf = leaf->next;
			continue;
		}
		/* Check for ACL topic access. */
		rc2 = mosquitto_acl_check(db, leaf->context, topic, MOSQ_ACL_READ);
		if(rc2 == MOSQ_ERR_ACL_DENIED){
			leaf = leaf->next;
			continue;
		}else if(rc2 == MOSQ_ERR_SUCCESS){
			client_qos = leaf->qos;

			if(db->config->upgrade_outgoing_qos){
				msg_qos = client_qos;
			}else{
				if(qos > client_qos){
					msg_qos = client_qos;
				}else{
					msg_qos = qos;
				}
			}
			if(msg_qos){
				mid = _mosquitto_mid_generate(leaf->context);//这个mid很有可能是发送包的ID
			}else{
				mid = 0;
			}
			if(leaf->context->is_bridge){
				/* If we know the client is a bridge then we should set retain
				 * even if the message is fresh. If we don't do this, retained
				 * messages won't be propagated. */
				client_retain = retain;
			}else{//默认执行这个
				/* Client is not a bridge and this isn't a stale message so
				 * retain should be false. */
				client_retain = false;
			}
                        if(BigFile){//直接发送,不保存
                            if(!leaf->context->BigFile){
                                leaf->context->BigFile = _mosquitto_malloc( sizeof(struct _mosquitto_BigFile_msg) );
                                leaf->context->BigFile->next = NULL;
                                leaf->context->BigFile->mid = stored->msg.mid;
                            }else {
                                leaf->context->BigFile->mid = stored->msg.mid;
                            }
                            //发送packet
                            // pthread_mutex_lock(&mosq->current_out_packet_mutex); 这里差一个锁机制，不知道可不可以用这个
                            while(packet->to_process > 0){//发送一个包，可能包很长，一次没有发完
                                write_length = _mosquitto_net_write(leaf->context, &(packet->payload[packet->pos]), packet->to_process);
                                if(write_length > 0){
                                    packet->to_process -= write_length;
                                    packet->pos += write_length;
                                }else{
                                    if(errno == EAGAIN || errno == COMPAT_EWOULDBLOCK){
                                    //    pthread_mutex_unlock(&mosq->current_out_packet_mutex);
                                        return MOSQ_ERR_SUCCESS;
                                    }else{
                                    //    pthread_mutex_unlock(&mosq->current_out_packet_mutex);
                                        switch(errno){
                                                case COMPAT_ECONNRESET:
                                                        return MOSQ_ERR_CONN_LOST;
                                                default:
                                                        return MOSQ_ERR_ERRNO;
                                        }
                                    } 
                                }
                             }
                             return 0; 
                        }else {
			     if(mqtt3_db_message_insert(db, leaf->context, mid, mosq_md_out, msg_qos, client_retain, stored) == 1) 
                                 rc = 1;
                        }
		}else{
			return 1; /* Application error */
		}
		leaf = leaf->next;
	}
	return rc;
}

static int _sub_topic_tokenise(const char *subtopic, struct _sub_token **topics)  //切割topic
{
	struct _sub_token *new_topic, *tail = NULL;
	int len;
	int start, stop, tlen;
	int i;

	assert(subtopic);
	assert(topics);

	if(subtopic[0] != '$'){
		new_topic = _mosquitto_malloc(sizeof(struct _sub_token));
		if(!new_topic) goto cleanup;
		new_topic->next = NULL;
		new_topic->topic = _mosquitto_strdup("");
		if(!new_topic->topic) goto cleanup;

		*topics = new_topic;
		tail = new_topic;
	}

	len = strlen(subtopic);

	if(subtopic[0] == '/'){
		new_topic = _mosquitto_malloc(sizeof(struct _sub_token));
		if(!new_topic) goto cleanup;
		new_topic->next = NULL;
		new_topic->topic = _mosquitto_strdup("");
		if(!new_topic->topic) goto cleanup;

		*topics = new_topic;
		tail = new_topic;

		start = 1;
	}else{
		start = 0;
	}

	stop = 0;
	for(i=start; i<len+1; i++){
		if(subtopic[i] == '/' || subtopic[i] == '\0'){
			stop = i;
			new_topic = _mosquitto_malloc(sizeof(struct _sub_token));
			if(!new_topic) goto cleanup;
			new_topic->next = NULL;

			if(start != stop){
				tlen = stop-start + 1;

				new_topic->topic = _mosquitto_calloc(tlen, sizeof(char));
				if(!new_topic->topic) goto cleanup;
				memcpy(new_topic->topic, &subtopic[start], tlen-1);
			}else{
				new_topic->topic = _mosquitto_strdup("");
				if(!new_topic->topic) goto cleanup;
			}
			if(tail){
				tail->next = new_topic;
				tail = tail->next;
			}else{
				tail = new_topic;
				*topics = tail;
			}
			start = i+1;
		}
	}

	return MOSQ_ERR_SUCCESS;

cleanup:
	tail = *topics;
	*topics = NULL;
	while(tail){
		if(tail->topic) _mosquitto_free(tail->topic);
		new_topic = tail->next;
		_mosquitto_free(tail);
		tail = new_topic;
	}
	return 1;
}

static int _sub_add(struct mosquitto_db *db, struct mosquitto *context, int qos, struct _mosquitto_subhier *subhier, struct _sub_token *tokens)
{
	struct _mosquitto_subhier *branch, *last = NULL;
	struct _mosquitto_subleaf *leaf, *last_leaf;

	if(!tokens){
		if(context){
			leaf = subhier->subs;
			last_leaf = NULL;
			while(leaf){
				if(!strcmp(leaf->context->id, context->id)){
					/* Client making a second subscription to same topic. Only
					 * need to update QoS. Return -1 to indicate this to the
					 * calling function. */
					leaf->qos = qos;
					return -1;
				}
				last_leaf = leaf;
				leaf = leaf->next;
			}
			leaf = _mosquitto_malloc(sizeof(struct _mosquitto_subleaf));
			if(!leaf) return MOSQ_ERR_NOMEM;
			leaf->next = NULL;
			leaf->context = context;
			leaf->qos = qos;
			if(last_leaf){
				last_leaf->next = leaf;
				leaf->prev = last_leaf;
			}else{
				subhier->subs = leaf;
				leaf->prev = NULL;
			}
			db->subscription_count++;
		}
		return MOSQ_ERR_SUCCESS;
	}

	branch = subhier->children;
	while(branch){
		if(!strcmp(branch->topic, tokens->topic)){
			return _sub_add(db, context, qos, branch, tokens->next);
		}
		last = branch;
		branch = branch->next;
	}
	/* Not found */
	branch = _mosquitto_calloc(1, sizeof(struct _mosquitto_subhier));
	if(!branch) return MOSQ_ERR_NOMEM;
	branch->topic = _mosquitto_strdup(tokens->topic);
	if(!branch->topic){
		_mosquitto_free(branch);
		return MOSQ_ERR_NOMEM;
	}
	if(!last){ //如果subhier没有孩子节点，则执行这里
		subhier->children = branch;
	}else{//否则作为subhier的最后一个节点的下一个兄弟
		last->next = branch;
	}
	return _sub_add(db, context, qos, branch, tokens->next);
}

static int _sub_remove(struct mosquitto_db *db, struct mosquitto *context, struct _mosquitto_subhier *subhier, struct _sub_token *tokens)
{
	struct _mosquitto_subhier *branch, *last = NULL;
	struct _mosquitto_subleaf *leaf;

	if(!tokens){
		leaf = subhier->subs;
		while(leaf){
			if(leaf->context==context){
				db->subscription_count--;
				if(leaf->prev){
					leaf->prev->next = leaf->next;
				}else{
					subhier->subs = leaf->next;
				}
				if(leaf->next){
					leaf->next->prev = leaf->prev;
				}
				_mosquitto_free(leaf);
				return MOSQ_ERR_SUCCESS;
			}
			leaf = leaf->next;
		}
		return MOSQ_ERR_SUCCESS;
	}

	branch = subhier->children;
	while(branch){
		if(!strcmp(branch->topic, tokens->topic)){
			_sub_remove(db, context, branch, tokens->next);
			if(!branch->children && !branch->subs && !branch->retained){
				if(last){
					last->next = branch->next;
				}else{
					subhier->children = branch->next;
				}
				_mosquitto_free(branch->topic);
				_mosquitto_free(branch);
			}
			return MOSQ_ERR_SUCCESS;
		}
		last = branch;
		branch = branch->next;
	}
	return MOSQ_ERR_SUCCESS;
}

static void _sub_search(struct mosquitto_db *db, struct _mosquitto_subhier *subhier, struct _sub_token *tokens, const char *source_id, const char *topic, int qos, int retain, struct mosquitto_msg_store *stored, bool set_retain, bool BigFile)
{
	/* FIXME - need to take into account source_id if the client is a bridge */
	struct _mosquitto_subhier *branch;
	bool sr;

	branch = subhier->children;
	while(branch){
		sr = set_retain;

		if(tokens && tokens->topic && (!strcmp(branch->topic, tokens->topic) || !strcmp(branch->topic, "+"))){
			/* The topic matches this subscription.
			 * Doesn't include # wildcards */
			if(!strcmp(branch->topic, "+")){
				/* Don't set a retained message where + is in the hierarchy. */
				sr = false;
			}
			_sub_search(db, branch, tokens->next, source_id, topic, qos, retain, stored, sr, BigFile);
			if(!tokens->next){
				_subs_process(db, branch, source_id, topic, qos, retain, stored, sr, BigFile);
			}
		}else if(!strcmp(branch->topic, "#") && !branch->children){
			/* The topic matches due to a # wildcard - process the
			 * subscriptions but *don't* return. Although this branch has ended
			 * there may still be other subscriptions to deal with.
			 */
			_subs_process(db, branch, source_id, topic, qos, retain, stored, false, BigFile);
		}
		branch = branch->next;
	}
}

int mqtt3_sub_add(struct mosquitto_db *db, struct mosquitto *context, const char *sub, int qos, struct _mosquitto_subhier *root)
{
	int rc = 0;
	struct _mosquitto_subhier *subhier, *child;
	struct _sub_token *tokens = NULL, *tail;

	assert(root);
	assert(sub);

	if(_sub_topic_tokenise(sub, &tokens)) return 1;

	subhier = root->children;
	while(subhier){
		if(!strcmp(subhier->topic, tokens->topic)){
			rc = _sub_add(db, context, qos, subhier, tokens);
			break;
		}
		subhier = subhier->next;
	}
	if(!subhier){
		child = _mosquitto_malloc(sizeof(struct _mosquitto_subhier));
		if(!child){
			_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
			return MOSQ_ERR_NOMEM;
		}
		child->topic = _mosquitto_strdup(tokens->topic);
		if(!child->topic){
			_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
			return MOSQ_ERR_NOMEM;
		}
		child->subs = NULL;
		child->children = NULL;
		child->retained = NULL;
		if(db->subs.children){
			child->next = db->subs.children;
		}else{
			child->next = NULL;
		}
		db->subs.children = child;

		rc = _sub_add(db, context, qos, child, tokens);
	}

	while(tokens){
		tail = tokens->next;
		_mosquitto_free(tokens->topic);
		_mosquitto_free(tokens);
		tokens = tail;
	}
	/* We aren't worried about -1 (already subscribed) return codes. */
	if(rc == -1) rc = MOSQ_ERR_SUCCESS;
	return rc;
}

int mqtt3_sub_remove(struct mosquitto_db *db, struct mosquitto *context, const char *sub, struct _mosquitto_subhier *root)
{
	int rc = 0;
	struct _mosquitto_subhier *subhier;
	struct _sub_token *tokens = NULL, *tail;

	assert(root);
	assert(sub);

	if(_sub_topic_tokenise(sub, &tokens)) return 1;

	subhier = root->children;
	while(subhier){
                printf("remove topic:%s\n", subhier->topic);
		if(!strcmp(subhier->topic, tokens->topic)){
			rc = _sub_remove(db, context, subhier, tokens);
			break;
		}
		subhier = subhier->next;
	}

	while(tokens){
		tail = tokens->next;
		_mosquitto_free(tokens->topic);
		_mosquitto_free(tokens);
		tokens = tail;
	}

	return rc;
}

int mqtt3_db_messages_queue(struct mosquitto_db *db, const char *source_id, const char *topic, int qos, int retain, struct mosquitto_msg_store *stored)
{
	int rc = 0;
	struct _mosquitto_subhier *subhier;
	struct _sub_token *tokens = NULL, *tail;

	assert(db);
	assert(topic);

	if(_sub_topic_tokenise(topic, &tokens)) return 1;

	subhier = db->subs.children;
	while(subhier){
		if(!strcmp(subhier->topic, tokens->topic)){
			if(retain){
				/* We have a message that needs to be retained, so ensure that the subscription
				 * tree for its topic exists.
				 */
				_sub_add(db, NULL, 0, subhier, tokens); //存在则不处理，不存在则添加该主题:w
			}
			_sub_search(db, subhier, tokens, source_id, topic, qos, retain, stored, true, false);
		}
		subhier = subhier->next;
	}
	while(tokens){
		tail = tokens->next;
		_mosquitto_free(tokens->topic);
		_mosquitto_free(tokens);
		tokens = tail;
	}

	return rc;
}

int mqtt3_db_messages_subhier(struct mosquitto_db *db, const char *source_id, const char *topic, int qos, int retain, struct mosquitto_msg_store *stored)
{
        int rc = 0;
        struct _mosquitto_subhier *subhier;
        struct _sub_token *tokens = NULL, *tail;

        assert(db);
        assert(topic);

        if(_sub_topic_tokenise(topic, &tokens)) return 1;

        subhier = db->subs.children;
        while(subhier){
                if(!strcmp(subhier->topic, tokens->topic)){
                        if(retain){
                                /* We have a message that needs to be retained, so ensure that the subscription
                                 * tree for its topic exists.
                                 */
                                _sub_add(db, NULL, 0, subhier, tokens); //存在则不处理，不存在则添加该主题:w
                        }
                        _sub_search(db, subhier, tokens, source_id, topic, qos, retain, stored, true, true);
                }
                subhier = subhier->next;
        }
        while(tokens){
                tail = tokens->next;
                _mosquitto_free(tokens->topic);
                _mosquitto_free(tokens);
                tokens = tail;
        }

        return rc;   
}

static int _subs_clean_session(struct mosquitto_db *db, struct mosquitto *context, struct _mosquitto_subhier *root)
{
	int rc = 0;
	struct _mosquitto_subhier *child, *last = NULL;
	struct _mosquitto_subleaf *leaf, *next;

	if(!root) return MOSQ_ERR_SUCCESS;

	leaf = root->subs;
	while(leaf){
		if(leaf->context == context){
			db->subscription_count--;
			if(leaf->prev){
				leaf->prev->next = leaf->next;
			}else{
				root->subs = leaf->next;
			}
			if(leaf->next){
				leaf->next->prev = leaf->prev;
			}
			next = leaf->next;
			_mosquitto_free(leaf);
			leaf = next;
		}else{
			leaf = leaf->next;
		}
	}

	child = root->children;
	while(child){
		_subs_clean_session(db, context, child);
		if(!child->children && !child->subs && !child->retained){
			if(last){
				last->next = child->next;
			}else{
				root->children = child->next;
			}
			_mosquitto_free(child->topic);
			_mosquitto_free(child);
			if(last){
				child = last->next;
			}else{
				child = root->children;
			}
		}else{
			last = child;
			child = child->next;
		}
	}
	return rc;
}

/* Remove all subscriptions for a client.
 */
int mqtt3_subs_clean_session(struct mosquitto_db *db, struct mosquitto *context, struct _mosquitto_subhier *root)
{
	struct _mosquitto_subhier *child;

	child = root->children;
	while(child){
		_subs_clean_session(db, context, child);
		child = child->next;
	}

	return MOSQ_ERR_SUCCESS;
}

void mqtt3_sub_tree_print(struct _mosquitto_subhier *root, int level)//负责打印整个订阅树
{
	int i;
	struct _mosquitto_subhier *branch;
	struct _mosquitto_subleaf *leaf;
        struct _mosquitto_packet *packet; //allan adds
	for(i=0; i<level*2; i++){
		printf(" ");
	}
	printf("%s", root->topic);
	leaf = root->subs;
	while(leaf){
		if(leaf->context){
			printf(" (context->id:%s, qos:%d)", leaf->context->id, leaf->qos);
                        printf("last_mid:%d\n", leaf->context->last_mid);
                        packet = leaf->context->out_packet;
                        while(packet)
                        {
                            printf("packet payload:%s\n", packet->payload);
                            packet = packet->next;
                        }
                        packet = leaf->context->current_out_packet;
                        while(packet)
                        {
                            printf("packet payload:%s\n", packet->payload);
                            packet = packet->next;
                        }
		}else{
			printf(" (%s, qos:%d)", "", leaf->qos);
		}
		leaf = leaf->next;
	}
	if(root->retained){
		printf(" (r)");
	}
        printf("\n");
 
	branch = root->children;
	while(branch){
		mqtt3_sub_tree_print(branch, level+1);
		branch = branch->next;
	}
}

static int _retain_process(struct mosquitto_db *db, struct mosquitto_msg_store *retained, struct mosquitto *context, const char *sub, int sub_qos)
{
	int rc = 0;
	int qos;
	uint16_t mid;

	rc = mosquitto_acl_check(db, context, retained->msg.topic, MOSQ_ACL_READ);
	if(rc == MOSQ_ERR_ACL_DENIED){
		return MOSQ_ERR_SUCCESS;
	}else if(rc != MOSQ_ERR_SUCCESS){
		return rc;
	}

	qos = retained->msg.qos;

	if(qos > sub_qos) qos = sub_qos;
	if(qos > 0){
		mid = _mosquitto_mid_generate(context);
	}else{
		mid = 0;
	}
	return mqtt3_db_message_insert(db, context, mid, mosq_md_out, qos, true, retained);
}

static int _retain_search(struct mosquitto_db *db, struct _mosquitto_subhier *subhier, struct _sub_token *tokens, struct mosquitto *context, const char *sub, int sub_qos, int level)
{
	struct _mosquitto_subhier *branch;
	int flag = 0;

	branch = subhier->children;
	while(branch){
		/* Subscriptions with wildcards in aren't really valid topics to publish to
		 * so they can't have retained messages.
		 */
		if(!strcmp(tokens->topic, "#") && !tokens->next){ //遇到通配符“#”
			/* Set flag to indicate that we should check for retained messages
			 * on "foo" when we are subscribing to e.g. "foo/#" and then exit
			 * this function and return to an earlier _retain_search().
			 */
			flag = -1;
			if(branch->retained){ //处理当前topic对应的retained
				_retain_process(db, branch->retained, context, sub, sub_qos);
			}
			if(branch->children){  //递归处理子topic对应的retained
				_retain_search(db, branch, tokens, context, sub, sub_qos, level+1);
			}
		}else if(strcmp(branch->topic, "+") && (!strcmp(branch->topic, tokens->topic) || !strcmp(tokens->topic, "+"))){
			if(tokens->next){ //tokens还没有比较完
				if(_retain_search(db, branch, tokens->next, context, sub, sub_qos, level+1) == -1
						|| (!branch->next && tokens->next && !strcmp(tokens->next->topic, "#") && level>0)){
                                       //如果遇到了通配符“#”,或者遇到了当前树中topic已比较完，而仍遇到通配符“#”
					if(branch->retained){
						_retain_process(db, branch->retained, context, sub, sub_qos);
					}
				}
			}else{
				if(branch->retained){
					_retain_process(db, branch->retained, context, sub, sub_qos);
				}
			}
		}

		branch = branch->next;
	}
	return flag;
}

int mqtt3_retain_queue(struct mosquitto_db *db, struct mosquitto *context, const char *sub, int sub_qos)
{
	struct _mosquitto_subhier *subhier;
	struct _sub_token *tokens = NULL, *tail;

	assert(db);
	assert(context);
	assert(sub);

	if(_sub_topic_tokenise(sub, &tokens)) return 1;

	subhier = db->subs.children;
	while(subhier){
		if(!strcmp(subhier->topic, tokens->topic)){
			_retain_search(db, subhier, tokens, context, sub, sub_qos, 0);
			break;
		}
		subhier = subhier->next;
	}
	while(tokens){
		tail = tokens->next;
		_mosquitto_free(tokens->topic);
		_mosquitto_free(tokens);
		tokens = tail;
	}

	return MOSQ_ERR_SUCCESS;
}

