#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#define MAX_LEVEL 6

typedef struct Node Node;
typedef struct SkipList SkipList;

Node* createNode(int level, int key, int value);
SkipList* createSkipList();
int randomLevel();
bool insert(SkipList* skipList, int key, int value);
void display(SkipList* skipList);
bool search(SkipList* skipList, int key);
