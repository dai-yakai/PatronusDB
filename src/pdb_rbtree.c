#include "pdb_rbtree.h"

kvs_rbtree_t global_rbtree;
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
		// if (z->key) kvs_free(z->key, strlen(z->key) + 1);
        // if (z->value) kvs_free(z->value, strlen(z->value) + 1);

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


int kvs_rbtree_create(kvs_rbtree_t* inst){
	if (inst == NULL)	return 1;

	inst->nil = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
	inst->nil->color = BLACK;
	inst->root = inst->nil;

	return 0;
}

void kvs_rbtree_destroy(kvs_rbtree_t* inst){
	if (inst == NULL) return ;

	rbtree_node *node = NULL;

	while (!(node = inst->root)) {
		
		rbtree_node *mini = rbtree_mini(inst, node);
		
		rbtree_node *cur = rbtree_delete(inst, mini);
		
		if (cur->key) kvs_free(cur->key, strlen(cur->key) + 1);
        if (cur->value) kvs_free(cur->value, strlen(cur->value) + 1);

		kvs_free(cur, sizeof(rbtree_node));
		
	}

	kvs_free(inst->nil, sizeof(rbtree_node));

	return ;

}

int kvs_rbtree_set(kvs_rbtree_t* inst, char* key, char* value){
	// printf("kvs_rbtree_set: %s, %s\n", key, value);
    if (!inst || !key || !value) return -1;

    rbtree_node *exist_node = rbtree_search(inst, key);
	// 已经存在该节点，直接对该节点的value进行更新
    if (exist_node != inst->nil) {
        if (exist_node->value) {
            kvs_free(exist_node->value, strlen(exist_node->value) + 1);
        }
        exist_node->value = kvs_malloc(strlen(value) + 1);
        if (!exist_node->value) return -2;
        strcpy(exist_node->value, value);
        
        return 0; 
    }

    rbtree_node *node = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
    if (!node) return -2; 
        
    node->key = kvs_malloc(strlen(key) + 1);
    if (!node->key) {
        kvs_free(node, sizeof(rbtree_node)); 
        return -2;
    }
    strcpy(node->key, key);
    
    node->value = kvs_malloc(strlen(value) + 1);
    if (!node->value) {
        kvs_free(node->key, strlen(key) + 1); 
        kvs_free(node, sizeof(rbtree_node));
        return -2;
    }
    strcpy(node->value, value);

    rbtree_insert(inst, node);

    return 0;
}

char* kvs_rbtree_get(kvs_rbtree_t* inst, char* key){
	if (!inst || !key) return NULL;
	rbtree_node *node = rbtree_search(inst, key);
	if (!node) return NULL; // no exist
	if (node == inst->nil) return NULL;

	return node->value;
}

int kvs_rbtree_del(kvs_rbtree_t* inst, char* key){
	// printf("kvs_rbtree_del: %s\n", key);
	if (!inst || !key) return -1;

	rbtree_node *node = rbtree_search(inst, key);
	if (!node) return 1; // no exist
	
	rbtree_node *cur = rbtree_delete(inst, node);
	if (cur->key) {
        kvs_free(cur->key, strlen(cur->key) + 1);
    }
    if (cur->value) {
        kvs_free(cur->value, strlen(cur->value) + 1);
    }

	kvs_free(cur, sizeof(rbtree_node));

#if ENABLE_PRINT_RB
	kvs_print_rbtree(inst);
#endif

	return 0;
}

int kvs_rbtree_mod(kvs_rbtree_t* inst, char* key, char* value){
	if (!inst || !key || !value) return -1;

	rbtree_node *node = rbtree_search(inst, key);
	if (!node) return 1; // no exist
	if (node == inst->nil) return 1;
	
	if (node->value) {
        kvs_free(node->value, strlen(node->value) + 1);
    }

	node->value = kvs_malloc(strlen(value) + 1);
	if (!node->value) return -2;
	
	// memset(node->value, 0, strlen(value) + 1);
	strcpy(node->value, value);

#if ENABLE_PRINT_RB
	kvs_print_rbtree(inst);
#endif

	return 0;
}

int kvs_rbtree_exist(kvs_rbtree_t* inst, char* key){
	if (!inst || !key) return -1;

	rbtree_node *node = rbtree_search(inst, key);
	if (!node) return 1; // no exist
	if (node == inst->nil) return 1;

	return 0;
}

void kvs_print_rbtree(kvs_rbtree_t* inst){
	rbtree_traversal(inst, inst->root);
}

static void rbtree_dump_dfs(FILE *fp, rbtree_node *node, rbtree_node *nil)
{
	printf("rbtree_dump_dfs\n");
    if (node == nil) return;
	fprintf(fp, "*3\r\n$4\r\nRSET\r\n$%lu\r\n%s\r\n$%lu\r\n%s\r\n", strlen((char*)node->key), (char*)node->key, strlen((char*)node->value), (char*)node->value);
	fflush(fp);
    rbtree_dump_dfs(fp, node->left, nil);
    rbtree_dump_dfs(fp, node->right, nil);
}

void kvs_rbtree_dump(kvs_rbtree_t *tree, const char *file)
{
	printf("kvs_rbtree_dump\n");
    if (!tree || !file) return;

    FILE *fp = fopen(file, "w");
    if (!fp) {
        perror("fopen dump");
        return;
    }

    rbtree_dump_dfs(fp, tree->root, tree->nil);
    fclose(fp);
}

// int kvs_rbtree_load(kvs_rbtree_t *arr, const char *file){
//     if (!arr || !file) return -1;

//     FILE *fp = fopen(file, "r");
//     if (!fp) {
//         perror("fopen load");
//         return -2;
//     }

//     char cmd[16];
//     char key[1024];
//     char value[1024];

//     while (fscanf(fp, "%15s %127s %511s", cmd, key, value) == 3) {
//         if (strcmp(cmd, "RSET") == 0) {
//             kvs_rbtree_set(arr, key, value);
//         }
//     }

// 	// kvs_print_rbtree(arr);

//     fclose(fp);
//     return 0;
// }

int kvs_rbtree_load(kvs_rbtree_t *arr, const char *file) {
    if (!arr || !file) return -1;

    // 使用 "rb" 模式打开，防止 Windows/Linux 换行符自动转换干扰字节计数
    FILE *fp = fopen(file, "rb"); 
    if (!fp) {
        perror("fopen load");
        return -2;
    }

    char line_buf[128]; // 用于读取 *3\r\n 或 $4\r\n 这种头信息
    char cmd[128];
    char key[4096];     // 假设 key 最大长度，生产环境可动态分配
    char value[4096];   // 假设 value 最大长度
    
    int array_len;
    size_t data_len;

    // 循环读取每一条 RESP 消息
    while (fgets(line_buf, sizeof(line_buf), fp) != NULL) {
        
        // 检查是否以*开头 (例如: *3\r\n)
        if (line_buf[0] != '*') {
            continue; // 跳过非法行或空行
        }
        
        // 解析参数个数
        array_len = atoi(line_buf + 1);
        if (array_len < 3) {
            continue; 
        }

        // 解析CMD ($4\r\nRSET\r\n)
        if (fgets(line_buf, sizeof(line_buf), fp) == NULL) break;
        if (line_buf[0] != '$') break;
        data_len = atoi(line_buf + 1); // 获取长度

        if (data_len >= sizeof(cmd)) data_len = sizeof(cmd) - 1; // 保护缓冲区
        fread(cmd, 1, data_len, fp); // 读取内容
        cmd[data_len] = '\0';        // 添加字符串结束符
        CONSUME_CRLF(fp);            // 吃掉\r\n

        // 解析KEY
        if (fgets(line_buf, sizeof(line_buf), fp) == NULL) break;
        data_len = atoi(line_buf + 1);

        if (data_len >= sizeof(key)) data_len = sizeof(key) - 1;
        fread(key, 1, data_len, fp);
        key[data_len] = '\0';
        CONSUME_CRLF(fp);

        // 解析value
        if (fgets(line_buf, sizeof(line_buf), fp) == NULL) break;
        data_len = atoi(line_buf + 1);

        if (data_len >= sizeof(value)) data_len = sizeof(value) - 1;
        fread(value, 1, data_len, fp);
        value[data_len] = '\0';
        CONSUME_CRLF(fp);

        if (strcmp(cmd, "RSET") == 0) {
            kvs_rbtree_set(arr, key, value);
        }
    }

    fclose(fp);
    return 0;
}

/**
 * @brief 批量处理插入操作
 * 
 * @param tokens ["MSET", "key1", "value1", ...]
 * @param count key1 value1 key2 value2...的总个数
 * 
 * @return 0:操作成功(已经存在会自动更新value); < 0: ERROR
 */
int kvs_rbtree_mset(kvs_rbtree_t *arr, char** tokens, int count){
	int i;
	for (i = 1;  i < count; i = i + 2){
		char* key = tokens[i];
		char* value = tokens[i + 1];

		int ret = kvs_rbtree_set(arr, key, value);
		if (ret != 0){
			return ret;
		}
	}

	return 0;
}


#if 0
int main() {

	int keyArray[20] = {24,25,13,35,23, 26,67,47,38,98, 20,19,17,49,12, 21,9,18,14,15};

	rbtree *T = (rbtree *)malloc(sizeof(rbtree));
	if (T == NULL) {
		printf("malloc failed\n");
		return -1;
	}
	
	T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
	T->nil->color = BLACK;
	T->root = T->nil;

	rbtree_node *node = T->nil;
	int i = 0;
	for (i = 0;i < 20;i ++) {
		node = (rbtree_node*)malloc(sizeof(rbtree_node));
		node->key = keyArray[i];
		node->value = NULL;

		rbtree_insert(T, node);
		
	}

	rbtree_traversal(T, T->root);
	printf("----------------------------------------\n");

	for (i = 0;i < 20;i ++) {

		rbtree_node *node = rbtree_search(T, keyArray[i]);
		rbtree_node *cur = rbtree_delete(T, node);
		free(cur);

		rbtree_traversal(T, T->root);
		printf("----------------------------------------\n");
	}
	

	
}
#endif

