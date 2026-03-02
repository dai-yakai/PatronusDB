#include "pdb_rbtree.h"

pdb_rbtree_t global_rbtree;
rbtree_node *rbtree_mini(rbtree *T, rbtree_node *x) {
	// printf("rbtree_mini: %d\n",  x == NULL);
	while (x->left != T->nil) {
		x = x->left;
	}
	return x;
}

rbtree_node *rbtree_maxi(rbtree *T, rbtree_node *x) {
	while (x->right != T->nil) {
		x = x->right;
	}
	return x;
}

rbtree_node *rbtree_successor(rbtree *T, rbtree_node *x) {
	rbtree_node *y = x->parent;

	if (x->right != T->nil) {
		return rbtree_mini(T, x->right);
	}

	while ((y != T->nil) && (x == y->right)) {
		x = y;
		y = y->parent;
	}
	return y;
}


void rbtree_left_rotate(rbtree *T, rbtree_node *x) {

	rbtree_node *y = x->right;  // x  --> y  ,  y --> x,   right --> left,  left --> right

	x->right = y->left; //1 1
	if (y->left != T->nil) { //1 2
		y->left->parent = x;
	}

	y->parent = x->parent; //1 3
	if (x->parent == T->nil) { //1 4
		T->root = y;
	} else if (x == x->parent->left) {
		x->parent->left = y;
	} else {
		x->parent->right = y;
	}

	y->left = x; //1 5
	x->parent = y; //1 6
}


void rbtree_right_rotate(rbtree *T, rbtree_node *y) {

	rbtree_node *x = y->left;

	y->left = x->right;
	if (x->right != T->nil) {
		x->right->parent = y;
	}

	x->parent = y->parent;
	if (y->parent == T->nil) {
		T->root = x;
	} else if (y == y->parent->right) {
		y->parent->right = x;
	} else {
		y->parent->left = x;
	}

	x->right = y;
	y->parent = x;
}

void rbtree_insert_fixup(rbtree *T, rbtree_node *z) {

	while (z->parent->color == RED) { //z ---> RED
		if (z->parent == z->parent->parent->left) {
			rbtree_node *y = z->parent->parent->right;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;

				z = z->parent->parent; //z --> RED
			} else {

				if (z == z->parent->right) {
					z = z->parent;
					rbtree_left_rotate(T, z);
				}

				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_right_rotate(T, z->parent->parent);
			}
		}else {
			rbtree_node *y = z->parent->parent->left;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;

				z = z->parent->parent; //z --> RED
			} else {
				if (z == z->parent->left) {
					z = z->parent;
					rbtree_right_rotate(T, z);
				}

				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_left_rotate(T, z->parent->parent);
			}
		}
		
	}

	T->root->color = BLACK;
}


void rbtree_insert(rbtree *T, rbtree_node *z) {

	rbtree_node *y = T->nil;
	rbtree_node *x = T->root;

	while (x != T->nil) {
		y = x;
#if ENABLE_KEY_CHAR

		if (strcmp(z->key, x->key) < 0) {
			x = x->left;
		} else if (strcmp(z->key, x->key) > 0) {
			x = x->right;
		} else {
			return ;
		}

#else
		if (z->key < x->key) {
			x = x->left;
		} else if (z->key > x->key) {
			x = x->right;
		} else { //Exist
			return ;
		}
#endif
	}

	z->parent = y;
	if (y == T->nil) {
		T->root = z;
#if ENABLE_KEY_CHAR
	} else if (strcmp(z->key, y->key) < 0) {
#else
	} else if (z->key < y->key) {
#endif
		y->left = z;
	} else {
		y->right = z;
	}

	z->left = T->nil;
	z->right = T->nil;
	z->color = RED;

	rbtree_insert_fixup(T, z);
}

void rbtree_delete_fixup(rbtree *T, rbtree_node *x) {

	while ((x != T->root) && (x->color == BLACK)) {
		if (x == x->parent->left) {

			rbtree_node *w= x->parent->right;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;

				rbtree_left_rotate(T, x->parent);
				w = x->parent->right;
			}

			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {

				if (w->right->color == BLACK) {
					w->left->color = BLACK;
					w->color = RED;
					rbtree_right_rotate(T, w);
					w = x->parent->right;
				}

				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->right->color = BLACK;
				rbtree_left_rotate(T, x->parent);

				x = T->root;
			}

		} else {

			rbtree_node *w = x->parent->left;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;
				rbtree_right_rotate(T, x->parent);
				w = x->parent->left;
			}

			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {

				if (w->left->color == BLACK) {
					w->right->color = BLACK;
					w->color = RED;
					rbtree_left_rotate(T, w);
					w = x->parent->left;
				}

				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->left->color = BLACK;
				rbtree_right_rotate(T, x->parent);

				x = T->root;
			}

		}
	}

	x->color = BLACK;
}

rbtree_node *rbtree_delete(rbtree *T, rbtree_node *z) {

	rbtree_node *y = T->nil;
	rbtree_node *x = T->nil;

	if ((z->left == T->nil) || (z->right == T->nil)) {
		y = z;
	} else {
		y = rbtree_successor(T, z);
	}

	if (y->left != T->nil) {
		x = y->left;
	} else if (y->right != T->nil) {
		x = y->right;
	}

	x->parent = y->parent;
	if (y->parent == T->nil) {
		T->root = x;
	} else if (y == y->parent->left) {
		y->parent->left = x;
	} else {
		y->parent->right = x;
	}

	if (y != z) {
#if ENABLE_KEY_CHAR
		// if (z->key) pdb_free(z->key, strlen(z->key) + 1);
        // if (z->value) pdb_free(z->value, strlen(z->value) + 1);

		void *tmp = z->key;
		z->key = y->key;
		y->key = tmp;

		tmp = z->value;
		z->value= y->value;
		y->value = tmp;

#else
		z->key = y->key;
		z->value = y->value;
#endif
	}

	if (y->color == BLACK) {
		rbtree_delete_fixup(T, x);
	}

	return y;
}

rbtree_node *rbtree_search(rbtree *T, KEY_TYPE key) {
	// printf("rbtree_search: key, %s\n", key);
	rbtree_node *node = T->root;
	while (node != T->nil) {
#if ENABLE_KEY_CHAR
		if (strcmp(key, node->key) < 0) {
			node = node->left;
		} else if (strcmp(key, node->key) > 0) {
			node = node->right;
		} else {
			return node;
		}

#else
		if (key < node->key) {
			node = node->left;
		} else if (key > node->key) {
			node = node->right;
		} else {
			return node;
		}	
#endif
	}
	return T->nil;
}


void rbtree_traversal(rbtree *T, rbtree_node *node) {
	if (node != T->nil) {
		rbtree_traversal(T, node->left);
#if ENABLE_KEY_CHAR
		printf("key:%s, value:%s\n", node->key, (char *)node->value);
#else
		printf("key:%d, color:%d\n", node->key, node->color);
#endif
		rbtree_traversal(T, node->right);
	}
}


int pdb_rbtree_create(pdb_rbtree_t* inst){
	if (inst == NULL)	return 1;

	inst->nil = (rbtree_node*)pdb_malloc(sizeof(rbtree_node));
	if (inst->nil == NULL){
		return PDB_MALLOC_NULL;
	}
	inst->nil->color = BLACK;
	inst->root = inst->nil;

	return PDB_DATASTRUCTURE_OK;
}

void pdb_rbtree_destroy(pdb_rbtree_t* inst){
	if (inst == NULL) return ;

	rbtree_node *node = NULL;

	while (!(node = inst->root)) {
		
		rbtree_node *mini = rbtree_mini(inst, node);
		
		rbtree_node *cur = rbtree_delete(inst, mini);
		
		if (cur->key) pdb_free(cur->key, -1);
        if (cur->value) pdb_free(cur->value, -1);

		pdb_free(cur, -1);
		
	}

	pdb_free(inst->nil, -1);

	return ;

}

int pdb_rbtree_set(pdb_rbtree_t* inst, char* key, pdb_value* value){
    if (!inst || !key || !value) return -1;

    rbtree_node *exist_node = rbtree_search(inst, key);
    if (exist_node != inst->nil) {
        if (exist_node->value) {
			pdb_decre_value(exist_node->value);
        }
        exist_node->value = value;
		pdb_incre_value(exist_node->value);

        return PDB_DATASTRUCTURE_EXIST; 
    }

    rbtree_node *node = (rbtree_node*)pdb_malloc(sizeof(rbtree_node));
    if (!node) return PDB_MALLOC_NULL; 
        
    node->key = pdb_malloc(strlen(key) + 1);
    if (!node->key) {
        pdb_free(node, -1); 
        return PDB_MALLOC_NULL;
    }
    strcpy(node->key, key);

	node->value = value;
	pdb_incre_value(value);

    rbtree_insert(inst, node);

    return PDB_DATASTRUCTURE_OK;
}

pdb_value* pdb_rbtree_get(pdb_rbtree_t* inst, char* key){
	if (!inst || !key) return NULL;
	rbtree_node *node = rbtree_search(inst, key);
	if (!node) return NULL; // no exist
	if (node == inst->nil) return NULL;

	return node->value;
}

int pdb_rbtree_del(pdb_rbtree_t* inst, char* key){
	if (!inst || !key) return -1;

	rbtree_node *node = rbtree_search(inst, key);
	if (node == NULL) return PDB_DATASTRUCTURE_NOEXIST; // no exist
	
	rbtree_node *cur = rbtree_delete(inst, node);
	if (cur->key) {
        pdb_free(cur->key, -1);
    }
    if (cur->value) {
        pdb_decre_value(cur->value);
    }

	pdb_free(cur, -1);

	return PDB_DATASTRUCTURE_OK;
}

int pdb_rbtree_mod(pdb_rbtree_t* inst, char* key, pdb_value* value){
	if (!inst || !key || !value) return -1;

	rbtree_node *node = rbtree_search(inst, key);
	if (!node) return PDB_DATASTRUCTURE_EXIST; // no exist
	if (node == inst->nil) return PDB_DATASTRUCTURE_EXIST;
	
	if (node->value) {
		pdb_decre_value(node->value);
    }

	node->value = value;
	pdb_incre_value(value);

	return PDB_DATASTRUCTURE_OK;
}

int pdb_rbtree_exist(pdb_rbtree_t* inst, char* key){
	if (!inst || !key) return -1;

	rbtree_node *node = rbtree_search(inst, key);
	if (!node) return PDB_DATASTRUCTURE_NOEXIST; // no exist
	if (node == inst->nil) return PDB_DATASTRUCTURE_NOEXIST;

	return PDB_DATASTRUCTURE_EXIST;
}

void pdb_print_rbtree(pdb_rbtree_t* inst){
	rbtree_traversal(inst, inst->root);
}

/**
 * @brief 批量处理插入操作
 * 
 * @param tokens ["MSET", "key1", "value1", ...]
 * @param count key1 value1 key2 value2...的总个数
 * 
 * @return 0:操作成功(已经存在会自动更新value); < 0: ERROR
 */
int pdb_rbtree_mset(pdb_rbtree_t *arr, char** tokens, int count){
	int i;
	for (i = 1;  i < count; i = i + 2){
		char* key = tokens[i];
		char* raw_value = tokens[i + 1];
		pdb_value* value = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);

		int ret = pdb_rbtree_set(arr, key, value);
		pdb_decre_value(value);
		if (ret != 0){
			return ret;
		}
	}

	return PDB_DATASTRUCTURE_OK;
}