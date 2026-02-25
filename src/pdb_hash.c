#include "pdb_hash.h"

pdb_hash_t global_hash;

/*
* hash function:
* Return idx according to key
*/
static int _hash(char *key, int size) {
	unsigned long hash = 5381;
    int c;

    while ((c = *key++))
        hash = ((hash << 5) + hash) + c; 

    return (int)(hash % size);
}

static int _pdb_hash_resize(pdb_hash_t* hash, size_t new_size){
	if (new_size == hash->max_slots)	return 0;
	if (new_size < INIT_TABLE_SIZE)		new_size = INIT_TABLE_SIZE;
	// rehash
	hashnode_t** new_nodes = (hashnode_t**)pdb_malloc(sizeof(hashnode_t*) * new_size);
	assert(new_nodes != NULL);
	memset(new_nodes, 0, sizeof(hashnode_t*) * new_size);

	int i = 0;
	for (i = 0; i < hash->max_slots; i++){
		hashnode_t* node = hash->nodes[i];
		while(node != NULL){
			hashnode_t* next = node->next;
			int new_idx = _hash(node->key, new_size);
			node->next = new_nodes[new_idx];
			new_nodes[new_idx] = node;

			node = next;
		}
	}

	pdb_free(hash->nodes, -1);
	hash->nodes = new_nodes;
	hash->max_slots = new_size;

	return 1;
}

/**
 * 	Return NULL if not expanded
 */
static int _pdb_hash_expand(pdb_hash_t* hash){
	if ((double)hash->count / hash->max_slots < HASH_EXPAND_FACTOR){
		return 0;
	}

	size_t new_size = hash->count * 2;
	return _pdb_hash_resize(hash, new_size);
}

static int _pdb_hash_shrink(pdb_hash_t* hash){
	if ((double)hash->count / hash->max_slots > HASH_SHRINK_FACTOR){
		return 0;
	}

	size_t new_size = hash->count / 2;
	return _pdb_hash_resize(hash, new_size);
}


/**
 * Deep copy
 */
hashnode_t *_create_node(char *key, char *value) {
	hashnode_t *node = (hashnode_t*)pdb_malloc(sizeof(hashnode_t));
	assert(node != NULL);
	
	// deep copy
	char* kcopy = pdb_malloc(strlen(key) + 1);
	assert(kcopy != NULL);
	memset(kcopy, 0, strlen(key) + 1);
	strncpy(kcopy, key, strlen(key));
	node->key = kcopy;

	char *kvalue = pdb_malloc(strlen(value) + 1);
	assert(kvalue != NULL);
	memset(kvalue, 0, strlen(value) + 1);
	strncpy(kvalue, value, strlen(value));
	node->value = kvalue;
	
	node->next = NULL;

	return node;
}


int pdb_hash_create(pdb_hash_t *hash) {
	hash->nodes = (hashnode_t**)pdb_malloc(sizeof(hashnode_t*) * INIT_TABLE_SIZE);
	assert(hash->nodes != NULL);
	memset(hash->nodes, 0, sizeof(hashnode_t*) * INIT_TABLE_SIZE);

	hash->max_slots = INIT_TABLE_SIZE;
	hash->count = 0; 

	return PDB_OK;
}

pdb_hash_t* pdb_hash_create2(){
	pdb_hash_t* hash = (pdb_hash_t*)pdb_malloc(sizeof(pdb_hash_t));
	hash->nodes = (hashnode_t**)pdb_malloc(sizeof(hashnode_t*) * INIT_TABLE_SIZE);
	assert(hash->nodes != NULL);
	memset(hash->nodes, 0, sizeof(hashnode_t*) * INIT_TABLE_SIZE);

	hash->max_slots = INIT_TABLE_SIZE;
	hash->count = 0; 

	return hash;
}

void pdb_hash_destory(pdb_hash_t *hash) {
	int i = 0;
	for (i = 0; i < hash->max_slots; i++) {
		hashnode_t* node = hash->nodes[i];

		while (node != NULL) { // error
			hashnode_t *tmp = node;
			node = node->next;
			hash->nodes[i] = node;
			
			pdb_free(tmp, -1);
		}
	}

	pdb_free(hash->nodes, -1);
}


int pdb_hash_set(pdb_hash_t *hash, char *key, char *value) {
	_pdb_hash_expand(hash);

	int idx = _hash(key, hash->max_slots);
	hashnode_t *node = hash->nodes[idx];
    
#if 1
	while (node != NULL) {
        if (strcmp(node->key, key) == 0) { 
#if ENABLE_KEY_POINTER
            if (node->value) {
                pdb_free(node->value, -1); 
            }
            
            char *new_value = pdb_malloc(strlen(value) + 1);
            if (!new_value) return PDB_MALLOC_NULL;
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
	if (new_node == NULL){
		return PDB_MALLOC_NULL;
	}
	new_node->next = hash->nodes[idx];
	hash->nodes[idx] = new_node;
	
	hash->count++;

	return PDB_OK;
}


char* pdb_hash_get(pdb_hash_t *hash, char *key) {
	if (!hash || !key) return NULL;

	int idx = _hash(key, hash->max_slots);
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

	int idx = _hash(key, hash->max_slots);
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
	pdb_free(node->value, -1);

	char *kvalue = pdb_malloc(strlen(value) + 1);
	if (kvalue == NULL) return PDB_MALLOC_NULL;
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

	int idx = _hash(key, hash->max_slots);

	hashnode_t *head = hash->nodes[idx];
	if (head == NULL) return -1; // noexist
	// head node
	if (strcmp(head->key, key) == 0) {
		hashnode_t *tmp = head->next;
		hash->nodes[idx] = tmp;
		
		pdb_free(head, -1);
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
	pdb_free(tmp->key, -1);
	pdb_free(tmp->value, -1);
#endif
	pdb_free(tmp, -1);
	
	hash->count--;

	_pdb_hash_shrink(hash);

	return 0;
}

/**
 * Return 1 if not found; otherwise return 0;
 */
int pdb_hash_exist(pdb_hash_t *hash, char *key) {
	char *value = pdb_hash_get(hash, key);
	// NULL return 1
	if (!value) return 1;

	return 0;
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
