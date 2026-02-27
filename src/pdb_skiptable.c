#include "pdb_skiptable.h"

struct SkipList global_skiplist;
typedef struct Node {
    char* key;
    value_type value;
    struct Node** forward;
} Node;

typedef struct SkipList {
    int level;
    Node* header;
} SkipList;

Node* createNode(int level, char* key, value_type value) {
    Node* newNode = (Node*)pdb_malloc(sizeof(Node));
    newNode->key = key;
    newNode->value = value;
    pdb_incre_value(value);
    newNode->forward = (Node**)pdb_malloc((level + 1) * sizeof(Node*));
    
    return newNode;
}

SkipList* createSkipTable() {
    SkipList* skipList = (SkipList*)pdb_malloc(sizeof(SkipList));
    skipList->level = 0;
    
    skipList->header = createNode(MAX_LEVEL, NULL, NULL);
    
    for (int i = 0; i <= MAX_LEVEL; ++i) {
        skipList->header->forward[i] = NULL;
    }
    
   return skipList; 
}

int randomLevel() {
   int level = 0;
   while (rand() < RAND_MAX / 2 && level < MAX_LEVEL)
      level++;
   return level;
}

int pdb_skiptable_insert(SkipList* skipList, char* key, value_type value) {
   Node* update[MAX_LEVEL + 1];
   Node* current = skipList->header;

   for (int i = skipList->level; i >= 0; --i) {
      while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
         current = current->forward[i];
      update[i] = current;
   }

   current = current->forward[0];

   if (current == NULL || strcmp(current->key, key) != 0) {
      int level = randomLevel();

      if (level > skipList->level) {
         for (int i = skipList->level + 1; i <= level; ++i)
            update[i] = skipList->header;
         skipList->level = level;
      }

      Node* newNode = createNode(level, key, value);

      for (int i = 0; i <= level; ++i) {
         newNode->forward[i] = update[i]->forward[i];
         update[i]->forward[i] = newNode;
      }     
      return PDB_OK;
   } else {
       return PDB_ERROR;
   }
}

void display(SkipList* skipList) {
    printf("Skip List:\n");
    
    for (int i = 0; i <= skipList->level; ++i) {
        Node* node = skipList->header->forward[i];
        printf("Level %d: ", i);
        
        while (node != NULL) {
            printf("%s ", node->key);
            node = node->forward[i];
        }
        
        printf("\n");
    }
}

value_type pdb_skiptable_search(SkipList* skipList, char* key) {
    Node* current = skipList->header;

    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && current->forward[i]->key < key)
            current = current->forward[i];
    }

    current = current -> forward[0];

    if(current && strcmp(current->key,key) == 0){
        return current->value;
    }else{
        return NULL;
    }
}

int pdb_skiptable_delete(SkipList* skipList, char* key) {
    Node* update[MAX_LEVEL + 1];
    Node* current = skipList->header;

    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];

    if (current != NULL && strcmp(current->key, key) == 0) {
        for (int i = 0; i <= skipList->level; ++i) {
            if (update[i]->forward[i] != current) {
                break;
            }
            update[i]->forward[i] = current->forward[i];
        }


        // pdb_free(current->value, -1);
        pdb_free(current->key, -1);
        pdb_decre_value(current->value);
        pdb_free(current->forward, -1);
        pdb_free(current, -1);

        while (skipList->level > 0 && skipList->header->forward[skipList->level] == NULL) {
            skipList->level--;
        }

        return PDB_OK;
    }

    return PDB_ERROR;
}

int pdb_skiptable_mod(SkipList* SkipList, char* key, value_type new_value){
    Node* current = SkipList->header;

    for (int i = SkipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && 
               strcmp(current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    if (current != NULL && strcmp(current->key, key) == 0) {
        if (current->value) {
            // pdb_free(current->value, -1); 
            pdb_decre_value(current->value);
        }

        current->value = new_value;
        pdb_incre_value(new_value);

        return PDB_OK;
    }

    return PDB_ERROR;
}

int pdb_skiptable_mset(SkipList* SkipList, char** tokens, int count){
	int i;
	for (i = 1;  i < count; i = i + 2){
		char* key = tokens[i];
		char* raw_value = tokens[i + 1];
        pdb_value* v = pdb_create_value(raw_value, PDB_VALUE_TYPE_DEFAULT);
		int ret = pdb_skiptable_insert(SkipList, key, v);
		if (ret != PDB_OK){
			return ret;
		}
        pdb_decre_value(v);
	}

	return PDB_OK;
}