#include "pdb_hash.h"
// Key, Value --> 
// Modify 

pdb_hash_t global_hash;


//Connection 
// 'C' + 'o' + 'n'
static int _hash(char *key, int size) {

	if (!key) return -1;

	unsigned long hash = 5381;
    int c;

    while ((c = *key++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return (int)(hash % size);

}

hashnode_t *_create_node(char *key, char *value) {

	hashnode_t *node = (hashnode_t*)pdb_malloc(sizeof(hashnode_t));
	if (!node) return NULL;
	
#if ENABLE_KEY_POINTER
	char *kcopy = pdb_malloc(strlen(key) + 1);
	if (kcopy == NULL) return NULL;
	memset(kcopy, 0, strlen(key) + 1);
	strncpy(kcopy, key, strlen(key));

	node->key = kcopy;

	char *kvalue = pdb_malloc(strlen(value) + 1);
	if (kvalue == NULL) { 
		// pdb_free(kvalue);
		return NULL;
	}
	memset(kvalue, 0, strlen(value) + 1);
	strncpy(kvalue, value, strlen(value));

	node->value = kvalue;
	
#else
	strncpy(node->key, key, MAX_KEY_LEN);
	strncpy(node->value, value, MAX_VALUE_LEN);
#endif
	node->next = NULL;

	return node;
}


//
int pdb_hash_create(pdb_hash_t *hash) {

	if (!hash) return -1;

	hash->nodes = (hashnode_t**)pdb_malloc(sizeof(hashnode_t*) * MAX_TABLE_SIZE);
	if (!hash->nodes) return -1;

	memset(hash->nodes, 0, sizeof(hashnode_t*) * MAX_TABLE_SIZE);

	hash->max_slots = MAX_TABLE_SIZE;
	hash->count = 0; 

	return 0;
}

// 
void pdb_hash_destory(pdb_hash_t *hash) {

	if (!hash) return;

	int i = 0;
	for (i = 0;i < hash->max_slots;i ++) {
		hashnode_t *node = hash->nodes[i];

		while (node != NULL) { // error

			hashnode_t *tmp = node;
			node = node->next;
			hash->nodes[i] = node;
			
			pdb_free(tmp, sizeof(hashnode_t));
			
		}
	}

	pdb_free(hash->nodes, sizeof(hashnode_t));
	
}

// 5 + 2

// mp
int pdb_hash_set(pdb_hash_t *hash, char *key, char *value) {
	if (!hash || !key || !value) return -1;

	int idx = _hash(key, MAX_TABLE_SIZE);
    // printf("idx: %d\n", idx);
	hashnode_t *node = hash->nodes[idx];
    
#if 1
	while (node != NULL) {
        if (strcmp(node->key, key) == 0) { 
#if ENABLE_KEY_POINTER
            if (node->value) {
                pdb_free(node->value, strlen(node->value) + 1); 
            }
            
            char *new_value = pdb_malloc(strlen(value) + 1);
            if (!new_value) return -1;
            strcpy(new_value, value);
            node->value = new_value;
#else
            strncpy(node->value, value, MAX_VALUE_LEN);
#endif
            return 0;
        }
        node = node->next;
    }
#endif

	hashnode_t *new_node = _create_node(key, value);
	new_node->next = hash->nodes[idx];
	hash->nodes[idx] = new_node;
	
	hash->count ++;

	return 0;
}


char * pdb_hash_get(pdb_hash_t *hash, char *key) {

	if (!hash || !key) return NULL;

	int idx = _hash(key, MAX_TABLE_SIZE);

	hashnode_t *node = hash->nodes[idx];

	while (node != NULL) {

		if (strcmp(node->key, key) == 0) {
			return node->value;
		}

		node = node->next;
	}

	return NULL;

}


int pdb_hash_mod(pdb_hash_t *hash, char *key, char *value) {

	if (!hash || !key) return -1;

	int idx = _hash(key, MAX_TABLE_SIZE);

	hashnode_t *node = hash->nodes[idx];

	while (node != NULL) {

		if (strcmp(node->key, key) == 0) {
			break;
		}

		node = node->next;
	}

	if (node == NULL) {
		return 1;
	}

	// node --> 
	pdb_free(node->value, sizeof(char));

	char *kvalue = pdb_malloc(strlen(value) + 1);
	if (kvalue == NULL) return -2;
	memset(kvalue, 0, strlen(value) + 1);
	strncpy(kvalue, value, strlen(value));

	node->value = kvalue;
	
	return 0;
}

int pdb_hash_count(pdb_hash_t *hash) {
	return hash->count;
}

int pdb_hash_del(pdb_hash_t *hash, char *key) {
	if (!hash || !key) return -2;

	int idx = _hash(key, MAX_TABLE_SIZE);

	hashnode_t *head = hash->nodes[idx];
	if (head == NULL) return -1; // noexist
	// head node
	if (strcmp(head->key, key) == 0) {
		hashnode_t *tmp = head->next;
		hash->nodes[idx] = tmp;
		
		pdb_free(head, sizeof(hashnode_t));
		hash->count --;
		
		return 0;
	}

	hashnode_t *cur = head;
	while (cur->next != NULL) {
		if (strcmp(cur->next->key, key) == 0) break; // search node
		
		cur = cur->next;
	}

	if (cur->next == NULL) {
		
		return -1;
	}

	hashnode_t *tmp = cur->next;
	cur->next = tmp->next;
#if ENABLE_KEY_POINTER
	pdb_free(tmp->key, sizeof(char));
	pdb_free(tmp->value, sizeof(char));
#endif
	pdb_free(tmp, sizeof(hashnode_t));
	
	hash->count --;

	return 0;
}


int pdb_hash_exist(pdb_hash_t *hash, char *key) {

	char *value = pdb_hash_get(hash, key);
	if (!value) return 1;

	return 0;
	
}

void pdb_hash_dump(pdb_hash_t *h, const char *file) {
    if (h == NULL || file == NULL) {
        return;
    }

	FILE *fp = fopen(file, "w");
	if (fp == NULL){
		printf("fopen error\n");
	}
    // 遍历所有桶
    for (int i = 0; i < h->max_slots; ++i) {
        hashnode_t *node = h->nodes[i];
        while (node != NULL) {
#if ENABLE_KEY_POINTER
            if (node->key == NULL || node->value == NULL) {
                node = node->next;
                continue;
            }
#endif
            const char *key = node->key;
            const char *val = node->value;
            size_t klen = strlen(key);
            size_t vlen = strlen(val);

            fprintf(fp, "*3\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                    klen, key, vlen, val);

            node = node->next;
        }
    }
}

static int read_resp_bulk_string(FILE *fp, char *buffer, size_t max_len) {
    char line[64];
    if (fgets(line, sizeof(line), fp) == NULL) return -1;
    
    if (line[0] != '$') return -1;
    
    int len = atoi(line + 1);
    if (len < 0 || (size_t)len >= max_len) return -1; 

    if (fread(buffer, 1, len, fp) != (size_t)len) return -1;

    buffer[len] = '\0'; 

    char crlf[2];
    if (fread(crlf, 1, 2, fp) != 2) return -1;

    return len;
}

int pdb_hash_load(pdb_hash_t *arr, const char *file) {
	printf("file name: %s\n", file);
    if (!arr || !file) return -1;

    FILE *fp = fopen(file, "r");
    if (!fp) {
        perror("fopen load");
        return -2;
    }

    char *cmd_buf = (char *)malloc(64); 
    char *key_buf = (char *)malloc(HASH_BUFFER_LENGTH); 
    char *val_buf = (char *)malloc(HASH_BUFFER_LENGTH); 

    if (!cmd_buf || !key_buf || !val_buf) {
        fprintf(stderr, "Malloc failed for load buffers\n");
        if (cmd_buf) free(cmd_buf);
        if (key_buf) free(key_buf);
        if (val_buf) free(val_buf);
        fclose(fp);

        return -3;
    }

    char line[64]; 
    int ret = 0;

    while (fgets(line, sizeof(line), fp) != NULL) {
        if (line[0] != '*') continue;
        int argc = atoi(line + 1);
        if (argc != 3) {
            break; 
        }

        if (read_resp_bulk_string(fp, cmd_buf, 64) < 0) break;
        if (read_resp_bulk_string(fp, key_buf, HASH_BUFFER_LENGTH) < 0) break;
        if (read_resp_bulk_string(fp, val_buf, HASH_BUFFER_LENGTH) < 0) break;

        if (strcmp(cmd_buf, "HSET") == 0) {
            pdb_hash_set(arr, key_buf, val_buf);
        }
    }

    free(cmd_buf);
    free(key_buf);
    free(val_buf);
    fclose(fp);
    
    return ret;
}

int pdb_hash_mset(pdb_hash_t* arr, char** tokens, int count){
	int i;
	for (i = 1;  i < count; i = i + 2){
		char* key = tokens[i];
		char* value = tokens[i + 1];

		int ret = pdb_hash_set(arr, key, value);
		if (ret != 0){
			return ret;
		}
	}

	return 0;

}

#if 0
int main() {

	pdb_hash_create(&hash);

	pdb_hash_set(&hash, "Teacher1", "King");
	pdb_hash_set(&hash, "Teacher2", "Darren");
	pdb_hash_set(&hash, "Teacher3", "Mark");
	pdb_hash_set(&hash, "Teacher4", "Vico");
	pdb_hash_set(&hash, "Teacher5", "Nick");

	char *value1 = pdb_hash_get(&hash, "Teacher1");
	printf("Teacher1 : %s\n", value1);

	int ret = pdb_hash_mod(&hash, "Teacher1", "King1");
	printf("mode Teacher1 ret : %d\n", ret);
	
	char *value2 = pdb_hash_get(&hash, "Teacher1");
	printf("Teacher2 : %s\n", value1);

	ret = pdb_hash_del(&hash, "Teacher1");
	printf("delete Teacher1 ret : %d\n", ret);

	ret = pdb_hash_exist(&hash, "Teacher1");
	printf("Exist Teacher1 ret : %d\n", ret);

	pdb_hash_destory(&hash);

	return 0;
}

#endif


